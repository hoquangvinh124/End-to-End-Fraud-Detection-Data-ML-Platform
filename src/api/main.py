import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .features import FEATURE_COLUMNS, ONLINE_FEATURE_REFS
from .models import (
    ErrorResponse,
    HealthResponse,
    OnlineTransactionRequest,
    PredictionResponse,
    TransactionRequest,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global model/runtime variables
ml_model = None
onnx_session = None
onnx_input_name = None
model_version = None

MODEL_NAME = os.environ.get("MLFLOW_MODEL_NAME", "fraud-detection")
MODEL_ALIAS = os.environ.get("MLFLOW_MODEL_ALIAS", "champion")
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
FEAST_REPO_PATH = os.environ.get("FEAST_REPO_PATH", "feature_store")
MODEL_SOURCE = os.environ.get("MODEL_SOURCE", "local").lower()


def _try_load_onnx_from_mlflow() -> bool:
    """Load ONNX artifact from the MLflow model registry alias."""
    global onnx_session, onnx_input_name, model_version

    try:
        import onnxruntime as ort

        import mlflow

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        client = mlflow.tracking.MlflowClient()
        version = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
        onnx_path = mlflow.artifacts.download_artifacts(
            run_id=version.run_id,
            artifact_path="onnx/model.onnx",
        )
        onnx_session = ort.InferenceSession(
            onnx_path,
            providers=["CPUExecutionProvider"],
        )
        onnx_input_name = onnx_session.get_inputs()[0].name
        model_version = f"{MODEL_NAME}:{MODEL_ALIAS}@v{version.version}"
        logger.info("Loaded ONNX model from MLflow registry: %s", model_version)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not load ONNX model from MLflow registry: %s", exc)
        return False


def _load_local_pickle_model() -> None:
    """Fallback loader for local prototype models."""
    global ml_model, model_version

    models_dir = Path(os.environ.get("MODELS_DIR", "models"))
    if not models_dir.exists():
        raise FileNotFoundError(f"Models directory not found: {models_dir}")

    model_files = sorted(models_dir.glob("fraud_detection_*.pkl"), reverse=True)
    if not model_files:
        raise FileNotFoundError(f"No model files found in {models_dir}")

    model_path = model_files[0]
    logger.info("Loading local fallback model from: %s", model_path)
    ml_model = joblib.load(model_path)
    model_version = model_path.stem
    logger.info("Local model loaded successfully: %s", model_version)


def load_model():
    """Load ONNX model from MLflow registry, falling back to local pickle for dev/tests."""
    try:
        use_mlflow_onnx = MODEL_SOURCE in {"mlflow", "onnx"} or os.environ.get(
            "ENABLE_MLFLOW_ONNX"
        ) == "1"
        if not use_mlflow_onnx or not _try_load_onnx_from_mlflow():
            _load_local_pickle_model()

    except Exception as e:
        logger.error(f"Error loading model: {str(e)}")
        raise


def _predict_probability(input_data: pd.DataFrame) -> float:
    ordered = input_data[FEATURE_COLUMNS].copy()
    for bool_col in ["IS_WEEKEND", "IS_NIGHT"]:
        ordered[bool_col] = ordered[bool_col].astype(int)

    if onnx_session is not None:
        outputs = onnx_session.run(
            None,
            {onnx_input_name: ordered.astype(np.float32).to_numpy()},
        )
        probabilities = outputs[-1]
        if isinstance(probabilities, list):
            probabilities = probabilities[0]
        return float(np.asarray(probabilities)[0, 1])

    if ml_model is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Model not loaded",
        )
    return float(ml_model.predict_proba(ordered)[0, 1])


def _get_online_features(request: OnlineTransactionRequest) -> dict[str, float]:
    try:
        from feast import FeatureStore

        store = FeatureStore(repo_path=FEAST_REPO_PATH)
        result = store.get_online_features(
            features=ONLINE_FEATURE_REFS,
            entity_rows=[
                {
                    "customer_id": request.customer_id,
                    "terminal_id": request.terminal_id,
                }
            ],
        ).to_dict()
        features: dict[str, float] = {}
        for feature_ref in ONLINE_FEATURE_REFS:
            feature_name = feature_ref.split(":", 1)[1]
            values = result.get(feature_name)
            if not values or values[0] is None:
                raise ValueError(f"Missing online feature: {feature_name}")
            features[feature_name] = float(values[0])
        return features
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Online feature lookup failed: {exc}",
        ) from exc


def _online_request_to_frame(request: OnlineTransactionRequest) -> pd.DataFrame:
    features = _get_online_features(request)
    features["TX_AMOUNT"] = request.TX_AMOUNT
    features["IS_WEEKEND"] = request.TX_DATETIME.weekday() >= 5
    features["IS_NIGHT"] = request.TX_DATETIME.hour < 6 or request.TX_DATETIME.hour >= 22
    return pd.DataFrame([features])[FEATURE_COLUMNS]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events"""
    # Startup: Load model
    logger.info("Starting up API...")
    load_model()
    yield
    # Shutdown: Cleanup if needed
    logger.info("Shutting down API...")


# Initialize FastAPI app
app = FastAPI(
    title="Fraud Detection API",
    description="API for real-time credit card fraud detection using machine learning",
    version="1.0.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint"""
    return {
        "message": "Fraud Detection API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "predict": "/predict",
            "docs": "/docs",
            "redoc": "/redoc",
        },
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy" if (ml_model is not None or onnx_session is not None) else "unhealthy",
        model_loaded=ml_model is not None or onnx_session is not None,
        model_version=model_version if model_version else "not_loaded",
    )


@app.post(
    "/predict",
    response_model=PredictionResponse,
    status_code=status.HTTP_200_OK,
    tags=["Prediction"],
    responses={
        200: {"description": "Successful prediction"},
        400: {"model": ErrorResponse, "description": "Invalid input data"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def predict_fraud(transaction: TransactionRequest):
    """
    Predict fraud probability for a transaction

    - **TX_AMOUNT**: Transaction amount (must be positive)
    - **CUSTOMER_AVG_AMOUNT_WINDOW_1D/7D/30D**: Customer's average transaction amounts
    - **CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D/7D/30D**:
      Number of customer transactions
    - **TERMINAL_RISK_1DAY/7DAY/30DAY_WINDOW**: Terminal fraud risk scores (0-1)
    - **TERMINAL_NB_TX_1DAY/7DAY/30DAY_WINDOW**: Number of terminal transactions
    - **IS_WEEKEND**: Whether transaction occurred on weekend
    - **IS_NIGHT**: Whether transaction occurred at night (10pm-6am)
    """
    try:
        # Convert request to DataFrame with correct column order
        input_data = pd.DataFrame([transaction.model_dump()])[FEATURE_COLUMNS]

        # Get prediction probability
        fraud_probability = _predict_probability(input_data)

        # Get binary prediction (threshold = 0.5)
        is_fraud = bool(fraud_probability >= 0.5)

        logger.info(
            f"Prediction: fraud_prob={fraud_probability:.4f}, is_fraud={is_fraud}"
        )

        return PredictionResponse(
            is_fraud=is_fraud,
            fraud_probability=round(fraud_probability, 4),
            model_version=model_version,
        )

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid input data: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Prediction error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Prediction failed: {str(e)}",
        )


@app.post(
    "/predict-online",
    response_model=PredictionResponse,
    status_code=status.HTTP_200_OK,
    tags=["Prediction"],
)
async def predict_fraud_online(transaction: OnlineTransactionRequest):
    """Predict fraud using request-time attributes plus Feast/Redis online features."""
    input_data = _online_request_to_frame(transaction)
    fraud_probability = _predict_probability(input_data)
    is_fraud = bool(fraud_probability >= 0.5)
    return PredictionResponse(
        is_fraud=is_fraud,
        fraud_probability=round(fraud_probability, 4),
        model_version=model_version if model_version else "not_loaded",
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Custom HTTP exception handler"""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(error=exc.detail, detail=str(exc)).model_dump(
            exclude={"timestamp"}
        ),
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """General exception handler"""
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=ErrorResponse(
            error="Internal server error", detail=str(exc)
        ).model_dump(exclude={"timestamp"}),
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
