import os
from airflow import DAG
from airflow.providers.amazon.aws.operators.step_function import StepFunctionStartExecutionOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s



with DAG(
    dag_id="sar-eddy-detection",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["aws", "sar", "eddy", "data-production"],
) as dag:


    volume = k8s.V1Volume(
        name="test-volume",
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="test-volume"),
    )
    volume_mount = k8s.V1VolumeMount(
        name="test-volume", mount_path="/root/mount_file", sub_path=None, read_only=True
    )

    # Get step function input from JSON file
    k = KubernetesPodOperator(
      task_id="test_sar_eddy_docker",
      image="ghcr.io/mike-gangl/podaac-sar-eddy:main",
      volumes=[
        k8s.V1Volume(
            name="dshm",
            empty_dir=k8s.V1EmptyDirVolumeSource(medium="Memory", size_limit="2Gi") # Sets SHM to 2 GiB
        )
      ],
      volume_mounts=[
        k8s.V1VolumeMount(
            name="dshm",
            mount_path="/dev/shm"
        )
      ],
      #cmds=["/bin/sh"],
      #arguments=["-c", "echo hello world"]
      # name="test-error-message",
      # email="airflow@example.com",
      # email_on_failure=True,
    )
