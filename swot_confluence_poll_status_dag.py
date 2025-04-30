from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import logging
from airflow.exceptions import AirflowException
from report import aggregate_run_data
import boto3
from step_function_input import StepFunctionInput

@task(
    retries=144,  # 12 hours with 10-second intervals
    retry_delay=timedelta(seconds=10),
)
def check_step_function_status(**context):
    execution_arn = context["dag_run"].conf.get("executionArn")
    if not execution_arn:
        raise ValueError("Missing executionArn in DAG run config")
    
    # Get run data
    name = execution_arn.split(":")[-1]
    module_data, failure_data = aggregate_run_data(execution_arn, name, temporal_range=None)
    logging.info(f"Run data: {module_data}, {failure_data}")

    # Check status
    client = boto3.client("stepfunctions", region_name="us-west-2")
    status = client.describe_execution(executionArn=execution_arn)["status"]
    logging.info(f"Status: {status}")

    if status == "SUCCEEDED":
        # Update and save the Step Function input
        input_data = StepFunctionInput.from_json_file()
        input_data.increment_version()
        input_data.increment_counter()
        input_data.save_to_file()
        logging.info("✅ Step Function completed successfully and input updated.")
        return True
    elif status in ["FAILED", "ABORTED", "TIMED_OUT"]:
        raise AirflowException(f"❌ Execution failed: {status}")
    elif status == "RUNNING":
        raise Exception("Step Function still running — retrying...")  # Triggers retry
    else:
        raise AirflowException(f"⚠️ Unknown status: {status}")

with DAG(
    dag_id="swot_confluence_poll_status_dag",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["aws", "stepfunction", "monitor"],
) as dag:

    check_step_function_status()
