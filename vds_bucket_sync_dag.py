"""
DAG for invoking the sync_lambda AWS Lambda function to synchronize S3 buckets.

Modes (set via params['mode']):
- "copy": Copy all files from source to destination, skipping files that are the same unless ignore_is_same is True.
- "sync": Make destination exactly match source (copy missing/changed files, delete extras in destination).
- "upload_folder": Only copy files from a specific folder (requires 'folder' param).
- "delete_folder": Delete all files in a specific folder in destination (requires 'folder' param; source params ignored).

Other params:
- folder: Name of folder under virtual_collections/ for upload_folder or delete_folder modes.
- ignore_is_same: If True, always copy files even if they are the same (only for copy/upload_folder modes).
- source_bucket, source_prefix, dest_bucket, dest_prefix: S3 bucket and prefix settings.
"""

from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.models import Variable
import json
from airflow.operators.python import PythonOperator


# Default parameters for sync_lambda
DEFAULTS = {
    "source_bucket": "podaac-ops-services-cloud-optimizer",
    "source_prefix": "virtual_collections/",
    "dest_bucket": "podaac-uat-cumulus-public",
    "dest_prefix": "virtual_collections/",
    "ignore_is_same": False,
}

with DAG(
    dag_id="vds_bucket_sync_update",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=["aws", "lambda", "bucket-sync"],
    catchup=False,
    params={
        "mode": {
            "type": "string",
            "oneOf": [
                {
                    "const": "copy",
                    "title": "Copy - Copy folders from src that do not exist in dest"
                },
                {
                    "const": "sync",
                    "title": "Sync - Mirror src to dest (adds, updates, deletes)"
                },
                {
                    "const": "upload_folder",
                    "title": "Upload Folder - Upload a single vds collection folder"
                },
                {
                    "const": "delete_folder",
                    "title": "Delete Folder - Remove a single vds collection folder"
                }
            ],
            "default": "upload_folder",
            "description": "Select the operation mode to perform."
        },
        "folder": None,
        "ignore_is_same": False,
        "source_bucket": DEFAULTS["source_bucket"],
        "source_prefix": DEFAULTS["source_prefix"],
        "dest_bucket": DEFAULTS["dest_bucket"],
        "dest_prefix": DEFAULTS["dest_prefix"],
    },
) as dag:
    VALID_MODES = {"copy", "sync", "upload_folder", "delete_folder"}
    mode = dag.params.get("mode")
    folder = dag.params.get("folder", None)
    ignore_is_same = dag.params.get("ignore_is_same", False)
    source_bucket = dag.params.get("source_bucket", DEFAULTS["source_bucket"])
    source_prefix = dag.params.get("source_prefix", DEFAULTS["source_prefix"])
    dest_bucket = dag.params.get("dest_bucket", DEFAULTS["dest_bucket"])
    dest_prefix = dag.params.get("dest_prefix", DEFAULTS["dest_prefix"])

    def build_event_from_params():
        event = {
            "mode": mode,
            "source_bucket": source_bucket,
            "source_prefix": source_prefix,
            "dest_bucket": dest_bucket,
            "dest_prefix": dest_prefix,
        }
        if mode in ["copy", "upload_folder"]:
            event["ignore_is_same"] = ignore_is_same
        if mode in ["upload_folder", "delete_folder"] and folder:
            event["folder"] = folder
        if mode == "delete_folder":
            event.pop("source_bucket")
            event.pop("source_prefix")
            event.pop("ignore_is_same", None)
        return json.dumps(event)

    event_payload = build_event_from_params()

    lambda_task = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda_bucket_sync",
        function_name="virtualizarr-ops-s3-bucket-sync",
        payload=event_payload,
        aws_conn_id="aws_default",
        log_type="Tail",
    )

    def print_lambda_result(**context):
        result = context['ti'].xcom_pull(task_ids='invoke_lambda_bucket_sync')
        print("Lambda result:", result)
        return result

    print_result_task = PythonOperator(
        task_id="print_lambda_result",
        python_callable=print_lambda_result,
        provide_context=True,
    )

    lambda_task >> print_result_task
