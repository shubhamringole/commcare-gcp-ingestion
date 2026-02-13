import os
import json
import requests
from datetime import datetime
from flask import Flask, jsonify
from google.cloud import storage
from google.cloud import secretmanager

app = Flask(__name__)

BASE_URL = "https://www.commcarehq.org"
BUCKET_NAME = "commcare-raw-data"


def get_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.environ["GCP_PROJECT"]
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    return client.access_secret_version(name=name).payload.data.decode("UTF-8")


@app.route("/run")
def run_commcare_ingestion():
    # 1. Read secrets
    DOMAIN = get_secret("COMMCARE_DOMAIN")
    USERNAME = get_secret("COMMCARE_USERNAME")
    API_KEY = get_secret("COMMCARE_API_KEY")
    FORM_XMLNS = get_secret("COMMCARE_FORM_XMLNS")

    # 2. Pull data from CommCare
    limit = 100
    offset = 0
    all_forms = []

    while True:
        url = (
            f"{BASE_URL}/a/{DOMAIN}/api/v0.5/form/"
            f"?limit={limit}&offset={offset}&xmlns={FORM_XMLNS}"
        )

        response = requests.get(url, auth=(USERNAME, API_KEY), timeout=30)
        response.raise_for_status()

        data = response.json()
        batch = data.get("objects", [])

        if not batch:
            break

        all_forms.extend(batch)
        offset += limit

    # 3. Write raw JSON to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
    blob_path = (
        f"forms/tg_registration/"
        f"ingestion_date={ingestion_date}/data.json"
    )

    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        json.dumps(all_forms),
        content_type="application/json"
    )

    return jsonify({
        "status": "success",
        "records_loaded": len(all_forms),
        "gcs_path": blob_path
    })


@app.route("/")
def health():
    return "CommCare ingestion service running"
