import urllib.request
import json
import time


def fetch_avro_schema(
    sr_url: str, subject: str, retries: int = 30, delay: int = 5
) -> str:
    """Fetch latest Avro schema string from Confluent Schema Registry.

    Retries to handle the window between connector registration and
    Debezium's first snapshot message (which triggers schema registration).
    """
    url = f"{sr_url}/subjects/{subject}/versions/latest"
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url) as resp:
                return json.loads(resp.read())["schema"]
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                print(
                    f"Schema not yet registered for {subject!r}, "
                    f"retrying ({attempt + 1}/{retries})…"
                )
                time.sleep(delay)
            else:
                raise
        except urllib.error.URLError as exc:
            print(
                f"Schema Registry unreachable: {exc}, "
                f"retrying ({attempt + 1}/{retries})…"
            )
            time.sleep(delay)
    raise RuntimeError(
        f"Could not fetch Avro schema for subject {subject!r} after {retries} attempts"
    )