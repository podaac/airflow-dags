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

from datetime import datetime, timezone, timedelta
from datetime import datetime

import boto3
import os
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
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
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.time_delta import TimeDeltaSensor

venue = os.environ.get("VENUE", "SIT").lower()
cluster_name = f"service-virtualzarr-gen-{venue}-cluster"
cluster_subnets = ["subnet-04fb3675968744380","subnet-0adee3417fedb7f05","subnet-0d15606f25bd4047b"]
default_sg = os.environ.get("SECURITY_GROUP_ID", "sg-09e578df0adec589e") 

def has_ec2_instances_in_cluster(**context):
    ecs_client = boto3.client('ecs')
    ec2_client = boto3.client('ec2')
    response = ecs_client.list_container_instances(cluster=cluster_name)
    container_instance_arns = response.get('containerInstanceArns', [])
    if not container_instance_arns:
        return 'no_ec2_instances'
    desc = ecs_client.describe_container_instances(cluster=cluster_name, containerInstances=container_instance_arns)
    ec2_instance_ids = [ci['ec2InstanceId'] for ci in desc['containerInstances'] if 'ec2InstanceId' in ci]
    if not ec2_instance_ids:
        return 'no_ec2_instances'
    ec2_desc = ec2_client.describe_instances(InstanceIds=ec2_instance_ids)
    now = datetime.now(timezone.utc)
    for reservation in ec2_desc['Reservations']:
        for instance in reservation['Instances']:
            launch_time = instance['LaunchTime']
            if (now - launch_time) < timedelta(minutes=5):
                return 'no_ec2_instances'
    return 'run_task'

with DAG(
    dag_id="podaac_ecs_cloud_optimized_generator_cold_start",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["aws", "ecs", "cloud-optimized"],
    params={'collection_id': 'default_value', 'loadable_coordinate_variables': 'lat,lon,time', 'output_bucket':'podaac-sit-services-cloud-optimizer', 'SSM_EDL_PASSWORD':'generate-edl-password', 'SSM_EDL_USERNAME':'generate-edl-username', 'START_DATE':'', 'END_DATE':''},
    catchup=False,
) as dag:

    check_ec2 = BranchPythonOperator(
        task_id='check_ec2_instances',
        python_callable=has_ec2_instances_in_cluster,
        provide_context=True,
    )

    dummy_ecs_task = EcsRunTaskOperator(
        task_id="no_ec2_instances",
        cluster=cluster_name,
        deferrable=True,
        task_definition="arn:aws:ecs:us-west-2:206226843404:task-definition/service-virtualzarr-gen-sit-app-task",
        capacity_provider_strategy=[{"capacityProvider":"service-virtualzarr-gen-sit-ecs-capacity-provider"}],
        overrides={
            "containerOverrides": [
                {
                    "name": "cloud-optimization-generation",
                    "command": ["echo", "dummy task to trigger EC2"],
                    "environment":[
                        {"name": 'COLLECTION', "value": "dummy"},
                        {"name": 'LOADABLE_VARS', "value": "dummy"},
                        {"name": 'OUTPUT_BUCKET', "value": "dummy"},
                        {"name": 'SSM_EDL_PASSWORD', "value": "dummy"},
                        {"name": 'SSM_EDL_USERNAME', "value": "dummy"}
                    ]
                },
            ],
        },
        network_configuration={
            "awsvpcConfiguration": {
                "securityGroups": [default_sg],
                "subnets": cluster_subnets,
            },
        },
        container_name="cloud-optimization-generation",
    )

  # We need to set container name here as it will not be returned if the status is provisioning (sometimes?).
  # https://github.com/apache/airflow/issues/51429
  run_task = EcsRunTaskOperator(
        task_id="run_task",
        cluster=cluster_name,
        deferrable=True,
        task_definition="arn:aws:ecs:us-west-2:206226843404:task-definition/service-virtualzarr-gen-sit-app-task",
        capacity_provider_strategy=[{"capacityProvider":"service-virtualzarr-gen-sit-ecs-capacity-provider"}],
        overrides={
        "containerOverrides": [
            {
                "name": "cloud-optimization-generation",
                "environment":[
			            {
                        	'name': 'COLLECTION',
                        	'value': "{{params.collection_id}}"
                    	},
                        {
                            'name': 'LOADABLE_VARS',
                            'value': "{{params.loadable_coordinate_variables}}"
                        },
                        {
                            'name': 'OUTPUT_BUCKET',
                            'value': "{{params.output_bucket}}"
                        },
                        {
                            'name': 'SSM_EDL_PASSWORD',
                            'value': "{{params.SSM_EDL_PASSWORD}}"
                        },
                        {
                            'name': 'SSM_EDL_USERNAME',
                            'value': "{{params.SSM_EDL_USERNAME}}"
                        },
                        {
                            'name': 'START_DATE',
                            'value': "{{params.START_DATE}}"
                        },
                        {
                            'name': 'END_DATE',
                            'value': "{{params.END_DATE}}"
                        }
		]
        #        #"command": ["echo hello world"],
            },
        ],
        },
	#wait_for_completion=False,
	network_configuration={
          "awsvpcConfiguration": {
              "securityGroups": [default_sg],
              "subnets": cluster_subnets,
          },
	},
        # [START howto_awslogs_ecs]
        #awslogs_group=log_group_name,
        #awslogs_region=aws_region,
        #awslogs_stream_prefix=f"ecs/{container_name}",
        # [END howto_awslogs_ecs]
        container_name="cloud-optimization-generation",
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

    sleep_5min = TimeDeltaSensor(
        task_id='wait_5_minutes',
        delta=timedelta(minutes=5),
    )

    check_ec2 >> run_task
    check_ec2 >> dummy_ecs_task >> sleep_5min >> run_task
