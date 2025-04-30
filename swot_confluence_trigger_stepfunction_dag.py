import os
from airflow import DAG
from airflow.providers.amazon.aws.operators.step_function import StepFunctionStartExecutionOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from step_function_input import StepFunctionInput

AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID_SIT")

with DAG(
    dag_id="swot_confluence_trigger_stepfunction_dag",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["aws", "stepfunction"],
) as dag:

    # Get step function input from JSON file
    step_function_input = StepFunctionInput.from_json_file().to_dict()


    print(f"Step function input: {step_function_input}")

    trigger_step_function = StepFunctionStartExecutionOperator(
        task_id="start_run_swot_confluence_stepfunction",
        state_machine_arn=f"arn:aws:states:us-west-2:{AWS_ACCOUNT_ID}:stateMachine:svc-confluence-sit-workflow",
        name="airflow-execution-{{ ts_nodash }}",  # unique execution name
        state_machine_input=step_function_input
    )

    trigger_polling = TriggerDagRunOperator(
        task_id="trigger_polling_dag",
        trigger_dag_id="swot_confluence_poll_status_dag",
        trigger_rule="all_done",
        execution_date="{{ execution_date }}",
        reset_dag_run=True,
        wait_for_completion=False,
        conf={"executionArn": "{{ task_instance.xcom_pull(task_ids='start_run_swot_confluence_stepfunction') }}"}
    )

    trigger_step_function >> trigger_polling
