import os
import logging
from datetime import datetime, timezone

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.step_function import StepFunctionStartExecutionOperator, StepFunctionGetExecutionOutputOperator
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from step_function_input import StepFunctionInput

AWS_ACCOUNT_ID_SIT = os.getenv("AWS_ACCOUNT_ID_SIT")


@dag(
    dag_id="swot_confluence_stepfunction_dag",
    description="Trigger Step Function and wait using deferrable operator",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["aws", "stepfunction", "trigger"],
)
def swot_confluence_stepfunction():

    @task()
    def prepare_input() -> dict:
        input_data = StepFunctionInput.from_s3("svc-confluence-sit")
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        input_data.temporal_range = f"&start_time=2020-09-01T00:00:00Z&end_time={now}&"
        input_data.increment_counter()
        input_data.save_to_s3("svc-confluence-sit")
        return input_data.to_dict()

    @task()
    def update_input():
        input_data = StepFunctionInput.from_s3("svc-confluence-sit")
        #input_data.increment_version()
        #input_data.run_type = "unconstrained" if input_data.run_type == "constrained" else "constrained"
        input_data.save_to_s3("svc-confluence-sit")
        logging.info("✅ Step Function completed successfully and input updated.")

    input_dict = prepare_input()

    start = StepFunctionStartExecutionOperator(
        task_id="start_step_function",
        state_machine_arn=f"arn:aws:states:us-west-2:{AWS_ACCOUNT_ID_SIT}:stateMachine:svc-confluence-sit-workflow-monitor-error",
        name="airflow-execution-{{ ts_nodash }}",
        state_machine_input=input_dict
        #deferrable=True,
    )

    get_output = StepFunctionGetExecutionOutputOperator(
        task_id="get_step_function_output",
        execution_arn="{{ task_instance.xcom_pull(task_ids='start_step_function') }}",
    )

    publish_cnm = BatchOperator(
        task_id='run_publish_cnm',
        job_name='svc-confluence-sit-publish-cnm-test-1',
        job_queue='svc-confluence-sit-publish-cnm',
        job_definition='svc-confluence-sit-publish-cnm',
        container_overrides={
            'command': [
                "-b", "svc-confluence-sit-sos",
                "-p", f"{input_dict['run_type']}/{input_dict['version']}",
                "-r", "podaac-services-sit",
                "-s", "podaac-dev-swot-sos"
            ]
        },
        aws_conn_id='podaac-services-sit',
    )

    input_dict >> start >> get_output >> publish_cnm >> update_input()

dag = swot_confluence_stepfunction()
