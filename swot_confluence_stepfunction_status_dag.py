import os
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
import boto3
from report import aggregate_run_data

AWS_ACCOUNT_ID_SIT = os.getenv("AWS_ACCOUNT_ID_SIT")
STATE_MACHINE_ARN = f"arn:aws:states:us-west-2:{AWS_ACCOUNT_ID_SIT}:stateMachine:svc-confluence-sit-workflow"

with DAG(
    dag_id="swot_confluence_report_status_dag",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",  # every 5 minutes
    catchup=False,
    tags=["aws", "stepfunction", "report"],
) as dag:

    @task
    def fetch_last_execution(state_machine_arn: str):
        client = boto3.client("stepfunctions", region_name="us-west-2")
        # Get the most recent execution regardless of status
        response = client.list_executions(
            stateMachineArn=state_machine_arn,
            maxResults=1
        )
        return [exec["executionArn"] for exec in response["executions"]]

    @task
    def report_status(execution_arn: str):
        client = boto3.client("stepfunctions", region_name="us-west-2")
        name = execution_arn.split(":")[-1]
        temporal_range = None

        try:
            execution = client.describe_execution(executionArn=execution_arn)
            status = execution["status"]
            print(f"[report] Execution: {execution_arn} | Status: {status}")

            module_data, failure_data = aggregate_run_data(execution_arn, name, temporal_range)
            print(f"[report] Module data: {module_data}")
            print(f"[report] Failure data: {failure_data}")
        except Exception as e:
            print(f"[report] Error analyzing {execution_arn}: {e}")

    # Task chaining
    execution_arn = fetch_last_execution(STATE_MACHINE_ARN)
    report_status.expand(execution_arn=execution_arn)
