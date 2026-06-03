from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import get_current_context


TARGET_DAG_ID = "podaac_ecs_cloud_optimized_generator"
CONFIG_FILE = Path(__file__).with_name("cloud_optimize_gen_cold_weekly_runs.json")

aws_account_id = os.getenv("AWS_ACCOUNT_ID")
venue = os.environ.get("VENUE", "SIT").lower()
cluster_name = f"service-virtualzarr-gen-{venue}-cluster"
cluster_subnets = Variable.get("cluster_subnets", deserialize_json=True)
default_sg = Variable.get("security_group_id")
logger = logging.getLogger(__name__)


def _load_runs() -> list[dict]:
    """Read the weekly run list from the JSON file."""
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_FILE.name}")

    with CONFIG_FILE.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, list):
        raise ValueError("weekly config file must contain a top-level JSON list")

    return payload


def has_running_ec2_capacity(min_age_minutes: int = 3) -> bool:
    """
    Returns True when the ECS cluster has at least one ACTIVE container instance
    backed by a running EC2 instance.
    """
    ecs_client = boto3.client("ecs")
    ec2_client = boto3.client("ec2")

    response = ecs_client.list_container_instances(cluster=cluster_name, status="ACTIVE")
    container_instance_arns = response.get("containerInstanceArns", [])
    if not container_instance_arns:
        return False

    description = ecs_client.describe_container_instances(
        cluster=cluster_name,
        containerInstances=container_instance_arns,
    )
    ec2_instance_ids = [
        instance["ec2InstanceId"]
        for instance in description.get("containerInstances", [])
        if instance.get("status") == "ACTIVE" and instance.get("ec2InstanceId")
    ]
    if not ec2_instance_ids:
        return False

    ec2_description = ec2_client.describe_instances(InstanceIds=ec2_instance_ids)
    now = datetime.now(timezone.utc)
    min_age = timedelta(minutes=min_age_minutes)

    for reservation in ec2_description.get("Reservations", []):
        for instance in reservation.get("Instances", []):
            launch_time = instance.get("LaunchTime")
            is_running = instance.get("State", {}).get("Name") == "running"
            if is_running and launch_time and (now - launch_time) >= min_age:
                return True
    return False


@task(task_id="warmup_ec2")
def launch_warmup_ec2() -> str:
    """Launch one warmup ECS task before triggering the real weekly runs."""
    ecs_client = boto3.client("ecs")

    try:
        ecs_client.run_task(
            cluster=cluster_name,
            taskDefinition=f"arn:aws:ecs:us-west-2:{aws_account_id}:task-definition/service-virtualzarr-gen-{venue}-app-task",
            capacityProviderStrategy=[
                {"capacityProvider": f"service-virtualzarr-gen-{venue}-ecs-capacity-provider"}
            ],
            tags=[
                {"key": "task_type", "value": "warmup"},
                {"key": "collection_id", "value": "warmup"},
            ],
            startedBy="weekly_controller_warmup",
            overrides={
                "containerOverrides": [
                    {
                        "name": "cloud-optimization-generation",
                        "environment": [
                            {"name": "COLLECTION", "value": "warmup"},
                            {"name": "LOADABLE_VARS", "value": "warmup"},
                            {"name": "OUTPUT_BUCKET", "value": "warmup"},
                            {"name": "SSM_EDL_PASSWORD", "value": "warmup"},
                            {"name": "SSM_EDL_USERNAME", "value": "warmup"},
                            {"name": "CPU_COUNT", "value": "1"},
                            {"name": "MEMORY_LIMIT", "value": "512MB"},
                            {"name": "BATCH_SIZE", "value": "1"},
                            {"name": "START_DATE", "value": ""},
                            {"name": "END_DATE", "value": ""},
                            {"name": "STAGING_BUCKET", "value": ""},
                        ],
                    }
                ]
            },
            networkConfiguration={
                "awsvpcConfiguration": {
                    "securityGroups": [default_sg],
                    "subnets": cluster_subnets,
                },
            },
        )
    except Exception:
        logger.exception("Warmup ECS launch failed, but continuing with the DAG run.")

    return "warmup_submitted"


with DAG(
    dag_id="podaac_ecs_cloud_optimized_generator_weekly_controller",
    description="Weekly controller that warms ECS once and triggers the cloud optimized generator DAG from a JSON list",
    start_date=datetime(2021, 1, 1),
    schedule="*/50 * * * *",
    catchup=False,
    tags=["aws", "ecs", "cloud-optimized", "controller", "weekly"],
) as dag:

    @task
    def build_trigger_kwargs() -> list[dict]:

        context = get_current_context()
        logical_date = context.get("logical_date") or datetime.now(timezone.utc)
        runs = _load_runs()

        trigger_kwargs: list[dict] = []
        for index, entry in enumerate(runs):
            if not isinstance(entry, dict):
                raise ValueError(f"runs[{index}] must be a JSON object")

            target_dag_id = entry.get("target_dag_id", TARGET_DAG_ID)
            conf = entry.get("conf", entry)
            if not isinstance(conf, dict):
                raise ValueError(f"runs[{index}].conf must be a JSON object")

            base_run_id = entry.get("trigger_run_id", target_dag_id)
            trigger_run_id = (
                f"{base_run_id}__{logical_date.strftime('%Y%m%dT%H%M%S')}__{index:02d}"
            )

            trigger_kwargs.append(
                {
                    "trigger_dag_id": target_dag_id,
                    "trigger_run_id": trigger_run_id,
                    "conf": conf,
                }
            )

        return trigger_kwargs

    warmup_ec2 = launch_warmup_ec2()
    wait_for_ec2_capacity = PythonSensor(
        task_id="wait_for_ec2_capacity",
        python_callable=has_running_ec2_capacity,
        poke_interval=30,
        timeout=15 * 60,
        mode="reschedule",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    trigger_cold_generator = TriggerDagRunOperator.partial(
        task_id="trigger_cloud_optimizer",
        wait_for_completion=False,
    ).expand_kwargs(build_trigger_kwargs())

    warmup_ec2 >> wait_for_ec2_capacity >> trigger_cold_generator
