import sys
from types import SimpleNamespace

import numpy as np
import pytest
from fastapi import HTTPException, status

import api.main as main


@pytest.fixture(autouse=True)
def reset_api_runtime(monkeypatch):
    monkeypatch.setattr(main, "onnx_session", None)
    monkeypatch.setattr(main, "onnx_input_name", None)
    monkeypatch.setattr(main, "ml_model", None)
    monkeypatch.setattr(main, "model_version", None)


def test_try_load_onnx_from_mlflow_uses_registry_alias(monkeypatch, tmp_path):
    model_path = tmp_path / "model.onnx"
    model_path.write_bytes(b"fake")

    class FakeClient:
        def get_model_version_by_alias(self, model_name, alias):
            assert model_name == "fraud-detection"
            assert alias == "champion"
            return SimpleNamespace(run_id="run-1", version="7")

    class FakeMlflow:
        tracking = SimpleNamespace(MlflowClient=FakeClient)

        @staticmethod
        def set_tracking_uri(uri):
            assert uri == main.MLFLOW_TRACKING_URI

        artifacts = SimpleNamespace(
            download_artifacts=lambda run_id, artifact_path: str(model_path)
        )

    class FakeSession:
        def __init__(self, path, providers):
            assert path == str(model_path)
            assert providers == ["CPUExecutionProvider"]

        def get_inputs(self):
            return [SimpleNamespace(name="float_input")]

    fake_ort = SimpleNamespace(InferenceSession=FakeSession)
    monkeypatch.setitem(sys.modules, "mlflow", FakeMlflow)
    monkeypatch.setitem(sys.modules, "onnxruntime", fake_ort)

    assert main._try_load_onnx_from_mlflow() is True
    assert main.onnx_input_name == "float_input"
    assert main.model_version == "fraud-detection:champion@v7"


def test_load_model_skips_registry_in_local_mode(monkeypatch):
    called = {"mlflow": False, "local": False}
    monkeypatch.setattr(main, "MODEL_SOURCE", "local")
    monkeypatch.setattr(
        main,
        "_try_load_onnx_from_mlflow",
        lambda: called.__setitem__("mlflow", True) or True,
    )
    monkeypatch.setattr(
        main,
        "_load_local_pickle_model",
        lambda: called.__setitem__("local", True),
    )

    main.load_model()

    assert called == {"mlflow": False, "local": True}


def test_load_model_uses_registry_when_enabled(monkeypatch):
    called = {"mlflow": False, "local": False}
    monkeypatch.setattr(main, "MODEL_SOURCE", "mlflow")
    monkeypatch.setattr(
        main,
        "_try_load_onnx_from_mlflow",
        lambda: called.__setitem__("mlflow", True) or True,
    )
    monkeypatch.setattr(
        main,
        "_load_local_pickle_model",
        lambda: called.__setitem__("local", True),
    )

    main.load_model()

    assert called == {"mlflow": True, "local": False}


def test_predict_probability_uses_onnx_session(monkeypatch, sample_valid_transaction):
    class FakeOnnxSession:
        def run(self, output_names, inputs):
            assert output_names is None
            assert "float_input" in inputs
            assert inputs["float_input"].dtype == np.float32
            return [np.array([[0.25, 0.75]], dtype=np.float32)]

    monkeypatch.setattr(main, "onnx_session", FakeOnnxSession())
    monkeypatch.setattr(main, "onnx_input_name", "float_input")
    monkeypatch.setattr(main, "ml_model", None)

    frame = main.pd.DataFrame([sample_valid_transaction])

    assert main._predict_probability(frame) == pytest.approx(0.75)


def test_predict_probability_requires_loaded_model(monkeypatch, sample_valid_transaction):
    monkeypatch.setattr(main, "onnx_session", None)
    monkeypatch.setattr(main, "ml_model", None)

    with pytest.raises(HTTPException) as exc_info:
        main._predict_probability(main.pd.DataFrame([sample_valid_transaction]))

    assert exc_info.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE


def test_get_online_features_success(monkeypatch):
    class FakeFeatureResult:
        def to_dict(self):
            return {
                feature_ref.split(":", 1)[1]: [float(index + 1)]
                for index, feature_ref in enumerate(main.ONLINE_FEATURE_REFS)
            }

    class FakeFeatureStore:
        def __init__(self, repo_path):
            assert repo_path == main.FEAST_REPO_PATH

        def get_online_features(self, features, entity_rows):
            assert features == main.ONLINE_FEATURE_REFS
            assert entity_rows == [{"customer_id": 1, "terminal_id": 2}]
            return FakeFeatureResult()

    monkeypatch.setitem(
        sys.modules,
        "feast",
        SimpleNamespace(FeatureStore=FakeFeatureStore),
    )
    request = main.OnlineTransactionRequest(
        customer_id=1,
        terminal_id=2,
        TX_AMOUNT=99.0,
        TX_DATETIME="2026-07-10T23:15:00",
    )

    features = main._get_online_features(request)

    assert features["CUSTOMER_AVG_AMOUNT_WINDOW_1D"] == 1.0
    assert features["TERMINAL_NB_TX_30DAY_WINDOW"] == 12.0


def test_get_online_features_missing_value_returns_503(monkeypatch):
    class FakeFeatureStore:
        def __init__(self, repo_path):
            pass

        def get_online_features(self, features, entity_rows):
            return SimpleNamespace(to_dict=lambda: {"CUSTOMER_AVG_AMOUNT_WINDOW_1D": [None]})

    monkeypatch.setitem(sys.modules, "feast", SimpleNamespace(FeatureStore=FakeFeatureStore))
    request = main.OnlineTransactionRequest(
        customer_id=1,
        terminal_id=2,
        TX_AMOUNT=99.0,
        TX_DATETIME="2026-07-10T23:15:00",
    )

    with pytest.raises(HTTPException) as exc_info:
        main._get_online_features(request)

    assert exc_info.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE


def test_predict_online_endpoint(client, mocker):
    mocker.patch(
        "api.main._get_online_features",
        return_value={
            feature_ref.split(":", 1)[1]: 1.0 for feature_ref in main.ONLINE_FEATURE_REFS
        },
    )
    mocker.patch("api.main._predict_probability", return_value=0.8)

    response = client.post(
        "/predict-online",
        json={
            "customer_id": 1,
            "terminal_id": 2,
            "TX_AMOUNT": 99.0,
            "TX_DATETIME": "2026-07-10T23:15:00",
        },
    )

    assert response.status_code == status.HTTP_200_OK
    assert response.json()["is_fraud"] is True
    assert response.json()["fraud_probability"] == 0.8
