# report_step_function_status_dag.py

import os
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
import boto3
from report import aggregate_run_data

AWS_ACCOUNT_ID_SIT = os.getenv("AWS_ACCOUNT_ID_SIT")
STATE_MACHINE_ARN = f"arn:aws:states:us-west-2:{AWS_ACCOUNT_ID_SIT}:stateMachine:svc-confluence-sit-workflow-monitor-error"

with DAG(
    dag_id="swot_confluence_report_status_dag",
    start_date=datetime(2024, 1, 1),
    schedule="*/1 * * * *",  # every 1 minute
    catchup=False,
    tags=["aws", "stepfunction", "report"],
) as dag:

    @task
    def fetch_running_executions(state_machine_arn: str):
        client = boto3.client("stepfunctions", region_name="us-west-2")
        response = client.list_executions(
            stateMachineArn=state_machine_arn,
            statusFilter="RUNNING"
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
    execution_arns = fetch_running_executions(STATE_MACHINE_ARN)
    report_status.expand(execution_arn=execution_arns)
