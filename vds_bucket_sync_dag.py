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

DAG for invoking the sync_lambda AWS Lambda function to synchronize S3 buckets.
"""

from __future__ import annotations

import json
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
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
    start_date=datetime(2021, 1, 1),
    tags=["aws", "lambda", "bucket-sync"],
    catchup=False,
    params={
        "mode": "upload_folder",
        "folder": "",
        "ignore_is_same": False,
        "source_bucket": DEFAULTS["source_bucket"],
        "source_prefix": DEFAULTS["source_prefix"],
        "dest_bucket": DEFAULTS["dest_bucket"],
        "dest_prefix": DEFAULTS["dest_prefix"],
    },
) as dag:

    @task
    def build_event_payload(**context):
        """
        Processes the runtime params and builds the JSON payload for Lambda.
        This runs at execution time, so it sees the values you enter in the UI.
        """
        params = context['params']
        mode = params['mode']
        folder = params.get('folder')
        
        # Base event structure
        event = {
            "mode": mode,
            "source_bucket": params['source_bucket'],
            "source_prefix": params['source_prefix'],
            "dest_bucket": params['dest_bucket'],
            "dest_prefix": params['dest_prefix'],
        }

        # Conditional Logic based on Mode
        if mode in ["copy", "upload_folder"]:
            event["ignore_is_same"] = params['ignore_is_same']
            
        if mode in ["upload_folder", "delete_folder"] and folder:
            event["folder"] = folder
            
        if mode == "delete_folder":
            # Clean up keys not required for deletion mode
            event.pop("source_bucket", None)
            event.pop("source_prefix", None)
            event.pop("ignore_is_same", None)

        return json.dumps(event)

    # 1. Generate the payload
    payload_data = build_event_payload()

    # 2. Invoke Lambda using the output of the previous task
    lambda_task = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda_bucket_sync",
        function_name="virtualizarr-ops-s3-bucket-sync",
        payload=payload_data,
        aws_conn_id="aws_default",
        log_type="Tail",
    )

    # 3. Handle the result
    def print_lambda_result(**context):
        result = context['ti'].xcom_pull(task_ids='invoke_lambda_bucket_sync')
        print("Lambda result:", result)
        return result

    print_result_task = PythonOperator(
        task_id="print_lambda_result",
        python_callable=print_lambda_result,
    )

    # Orchestration
    # payload_data (task) >> lambda_task is handled automatically by passing the variable
    lambda_task >> print_result_task