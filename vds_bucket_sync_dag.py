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
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.ecs import EcsClusterStates
from airflow.providers.amazon.aws.hooks.ecs import EcsTaskStates
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator

aws_account_id = os.getenv("AWS_ACCOUNT_ID")
venue = os.environ.get("VENUE", "SIT").lower()
cluster_name = f"service-virtualzarr-gen-{venue}-cluster"
cluster_subnets = Variable.get("cluster_subnets", deserialize_json=True)
default_sg = Variable.get("security_group_id")

with DAG(
    dag_id="vds_bucket_sync",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["aws", "lambda", "bucket-sync"],
    params={},
    catchup=False,
) as dag:

    invoke_lambda = AwsLambdaInvokeFunctionOperator(
        task_id="invoke_lambda_bucket_sync",
        function_name="virtualizarr-ops-s3-bucket-sync",
        payload={},  # Add your payload here if needed
        aws_conn_id="aws_default",
        log_type="Tail",
    )

    invoke_lambda
