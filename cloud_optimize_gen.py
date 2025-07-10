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
from airflow.utils.trigger_rule import TriggerRule

venue = os.environ.get("VENUE", "SIT").lower()
cluster_name = f"service-virtualzarr-gen-{venue}-cluster"

with DAG(
    dag_id="podaac_ecs_cloud_optimized_generator",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["aws", "ecs", "cloud-optimized"],
    params={'collection_id': 'default_value', 'loadable_coordinate_variables': 'lat,lon,time', 'output_bucket':'podaac-sit-services-cloud-optimizer', 'SSM_EDL_PASSWORD':'generate-edl-password', 'SSM_EDL_USERNAME':'generate-edl-username', 'START_DATE':'', 'END_DATE':''},
    catchup=False,
) as dag:

  run_task = EcsRunTaskOperator(
        task_id="run_task",
        cluster=cluster_name,
        deferrable=True,
        task_definition="arn:aws:ecs:us-west-2:206226843404:task-definition/service-virtualzarr-gen-sit-app-task:1",
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
                        }
		]
        #        #"command": ["echo hello world"],
            },
        ],
        },
	#wait_for_completion=False,
	network_configuration={"awsvpcConfiguration": {"subnets": ["subnet-04fb3675968744380","subnet-0adee3417fedb7f05","subnet-0d15606f25bd4047b"]}},
        # [START howto_awslogs_ecs]
        #awslogs_group=log_group_name,
        #awslogs_region=aws_region,
        #awslogs_stream_prefix=f"ecs/{container_name}",
        # [END howto_awslogs_ecs]
    )

  await_task_finish = EcsTaskStateSensor(
    task_id="await_task_finish",
    cluster=cluster_name,
    task=run_task.output["ecs_task_arn"],
    target_state=EcsTaskStates.STOPPED,
    failure_states={EcsTaskStates.NONE},
  )

  run_task >> await_task_finish
