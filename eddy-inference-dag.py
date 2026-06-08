import os
from airflow import DAG
from airflow.providers.amazon.aws.operators.step_function import StepFunctionStartExecutionOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow.operators.python import PythonOperator


from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s
from airflow.models import Variable



with DAG(
    dag_id="sar-eddy-inference",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={'SCENE_TARBALL': None},
    tags=["aws", "sar", "eddy", "data-production"],
) as dag:

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    pod_resources = k8s.V1ResourceRequirements(
            requests={
               "cpu": "8",
               "memory": "8Gi", 
               "ephemeral-storage": "20Gi"
            },  # Request 8 CPU cores, 32GB memory
            limits={
               "cpu": "12",
               "memory": "16Gi",
               "ephemeral-storage": "25Gi"
            },     # Limit to 12 CPU cores, 48GB memory
    )

    affinity = k8s.V1Affinity(
      node_affinity=k8s.V1NodeAffinity(
        preferred_during_scheduling_ignored_during_execution=[
            k8s.V1PreferredSchedulingTerm(
                weight=1,
                preference=k8s.V1NodeSelectorTerm(
                    match_expressions=[
                        k8s.V1NodeSelectorRequirement(key="karpenter.sh/capacity-type", operator="In", values=["on-demand"])
                    ]
                ),
            )
        ]
      )
    )
    
    k = KubernetesPodOperator(
      task_id="sar_eddy_run_inference",
      image="ghcr.io/podaac/podaac-sar-eddy:main",
      volumes=[
        k8s.V1Volume(
            name="dshm",
            empty_dir=k8s.V1EmptyDirVolumeSource(medium="Memory", size_limit="8Gi") # Sets SHM to 8 GiB
        )
      ],
      volume_mounts=[
        k8s.V1VolumeMount(
            name="dshm",
            mount_path="/dev/shm"
        )
      ],
      image_pull_policy="Always",
      startup_timeout_seconds=300, # wait 5 minutes for an isntance to startup and have the pod available
      env_vars={
            'FILE_NAME': "{{params.SCENE_TARBALL}}",
            'OUTPUT_BUCKET_NAME': '{{ var.value.PROCESS_OUTPUTS }}',
            'AWS_DEFAULT_REGION': 'us-west-2',
            'PYTHONUNBUFFERED' : "TRUE"
      },
      affinity=affinity,
      container_resources=pod_resources,
      log_events_on_failure=True,
      cmds=["sh", "run_inference.sh"],
      #arguments=["-c", "echo hello world"]
      # name="test-error-message",
      # email="airflow@example.com",
      # email_on_failure=True,
    )

    k
