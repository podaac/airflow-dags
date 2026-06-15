# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import boto3
import os
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.ecs import EcsClusterStates
from airflow.providers.amazon.aws.hooks.ecs import EcsTaskStates

from airflow.providers.amazon.aws.operators.ecs import (
    EcsCreateClusterOperator,
    EcsDeleteClusterOperator,
    EcsDeregisterTaskDefinitionOperator,
    EcsRegisterTaskDefinitionOperator,
    EcsRunTaskOperator,
)
from airflow.providers.amazon.aws.sensors.ecs import (
    EcsTaskStateSensor
)
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

aws_account_id = os.getenv("AWS_ACCOUNT_ID")
venue = os.environ.get("VENUE", "SIT").lower()
# Set default for staging_bucket if venue is 'ops'
default_staging_bucket = f"podaac-uat-cumulus-public" if venue == "ops" else ''
cluster_name = f"service-virtualzarr-gen-{venue}-cluster"
cluster_subnets = Variable.get("cluster_subnets", deserialize_json=True)
default_sg = Variable.get("security_group_id")
logger = logging.getLogger(__name__)


def conf_or_param(key: str) -> str:
    """Use dag_run.conf when provided, otherwise fall back to DAG params."""
    return f"{{{{ dag_run.conf.get('{key}', params.{key}) }}}}"


@task(task_id="warmup_ec2")
def launch_warmup_ec2() -> str:
    """Submit the warmup ECS task and always let the DAG continue."""
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
            startedBy="warmup_ec2",
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

with DAG(
    dag_id="podaac_ecs_cloud_optimized_generator_cold",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["aws", "ecs", "cloud-optimized"],
    params={
        'collection_id': 'default_value',
        'loadable_coordinate_variables': 'lat,lon,time',
        'output_bucket': f'podaac-{venue}-services-cloud-optimizer',
        'SSM_EDL_PASSWORD': 'generate-edl-password',
        'SSM_EDL_USERNAME': 'generate-edl-username',
        'START_DATE': '',
        'END_DATE': '',
        'CPU_COUNT': '96',
        'MEMORY_LIMIT': '6GB',
        'BATCH_SIZE': '96',
        'staging_bucket': default_staging_bucket
    },
    catchup=False,
) as dag:

    # Warm-up task to trigger EC2 capacity provisioning on a cold cluster.
    # Airflow marks this task successful once the ECS submission is made.
    warmup_ec2 = launch_warmup_ec2()

    # We need to set container name here as it will not be returned if the status is provisioning (sometimes?).
    # https://github.com/apache/airflow/issues/51429
    run_task = EcsRunTaskOperator(
        task_id="run_task",
        trigger_rule=TriggerRule.ALL_DONE,
        cluster=cluster_name,
        deferrable=True,
        task_definition=f"arn:aws:ecs:us-west-2:{aws_account_id}:task-definition/service-virtualzarr-gen-{venue}-app-task",
        capacity_provider_strategy=[
            {"capacityProvider": f"service-virtualzarr-gen-{venue}-ecs-capacity-provider"}],
        tags={
            "task_type": "run_task",
            "collection_id": conf_or_param("collection_id"),
        },
        overrides={
            "containerOverrides": [
              {
                  "name": "cloud-optimization-generation",
                  "environment": [
                        {
                          'name': 'COLLECTION',
                          'value': conf_or_param("collection_id")
                        },
                        {
                          'name': 'LOADABLE_VARS',
                          'value': conf_or_param("loadable_coordinate_variables")
                        },
                        {
                          'name': 'OUTPUT_BUCKET',
                          'value': conf_or_param("output_bucket")
                        },
                        {
                          'name': 'SSM_EDL_PASSWORD',
                          'value': conf_or_param("SSM_EDL_PASSWORD")
                        },
                        {
                          'name': 'SSM_EDL_USERNAME',
                          'value': conf_or_param("SSM_EDL_USERNAME")
                        },
                        {
                          'name': 'CPU_COUNT',
                          'value': conf_or_param("CPU_COUNT")
                        },
                        {
                          'name': 'MEMORY_LIMIT',
                          'value': conf_or_param("MEMORY_LIMIT")
                        },
                        {
                          'name': 'BATCH_SIZE',
                          'value': conf_or_param("BATCH_SIZE")
                        },
                        {
                          'name': 'START_DATE',
                          'value': conf_or_param("START_DATE")
                        },
                        {
                          'name': 'END_DATE',
                          'value': conf_or_param("END_DATE")
                        },
                        {
                          'name': 'STAGING_BUCKET',
                          'value': conf_or_param("staging_bucket")
                        }
                    ]
                }
            ]
        },
        # wait_for_completion=False,
        network_configuration={
            "awsvpcConfiguration": {
                "securityGroups": [default_sg],
                "subnets": cluster_subnets,
            },
        },
        # [START howto_awslogs_ecs]
        # awslogs_group=log_group_name,
        # awslogs_region=aws_region,
        # awslogs_stream_prefix=f"ecs/{container_name}",
        # [END howto_awslogs_ecs]
        container_name="cloud-optimization-generation",
    )

    wait_for_ec2_capacity = PythonSensor(
        task_id="wait_for_ec2_capacity",
        python_callable=has_running_ec2_capacity,
        poke_interval=30,
        timeout=15 * 60,
        mode="reschedule",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # Use the sensor below to monitor job
    #run_task.wait_for_completion = False

    # await_task_finish = EcsTaskStateSensor(
    #       task_id="await_task_finish",
    #       cluster=cluster_name,
    #       task=run_task.output["ecs_task_arn"],
    #       target_state=EcsTaskStates.STOPPED,
    #       failure_states={EcsTaskStates.NONE},
    # )

    warmup_ec2 >> wait_for_ec2_capacity >> run_task  # >> await_task_finish
