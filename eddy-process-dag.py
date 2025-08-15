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
    dag_id="sar-eddy-detection",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={'SSM_EDL_PASSWORD':'generate-edl-password', 'SSM_EDL_USERNAME':'generate-edl-username', 'START_DATE':'2019-01-01', 'END_DATE':'2019-01-10'},
    tags=["aws", "sar", "eddy", "data-production"],
) as dag:

    import asf_search as asf
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    def asf_search(start="2021-01-01", end="2021-03-31", **kwargs):

      dag_run_id = kwargs['dag_run'].run_id

      wkt = (
      "POLYGON((-124.5751 41.8841,-124.1137 44.432,-137.6691 44.2286,-138.3284 27.9612,-114.224 28.2756,-117.8871 33.7581,-122.1991 37.0915,-124.3064 40.3865,-124.2235 41.2264,-124.5751 41.8841))"
      )
      results = asf.geo_search(
          platform=[asf.PLATFORM.SENTINEL1],
          intersectsWith=wkt,
          maxResults=100000,
          beamMode=[asf.BEAMMODE.IW, asf.BEAMMODE.WV],
          processingLevel=[asf.PRODUCT_TYPE.GRD_HD],
          start=start,
          end=end,
      )
      with open('results.txt', 'w') as search_result_file:
        search_result_file.write(f"defaults:\n")
        search_result_file.write(f"  - default  # inherit hyp3/default.yaml\n\n")
        search_result_file.write(f"granules:\n")

        for result in results:
          search_result_file.write(f"  - {result.properties['sceneName']}\n")

      #write output file to S3
      temp_bucket = Variable.get("PROCESS_OUTPUTS")
      if temp_bucket is not None:
          s3_hook = S3Hook(aws_conn_id='aws_default') # Or your specific AWS connection
          key = f'/temp/{dag_run_id}/results.txt'
          s3_hook.load_file(filename="results.txt",key=key, bucket_name=temp_bucket)
          kwargs['ti'].xcom_push(key='search_results', value=key)
          #push the data...

    asf_search_task = PythonOperator(
            task_id='asf_search_task',
            python_callable=asf_search,
            op_args=["{{ params.START_DATE }}", "{{ params.END_DATE }}"], # Positional arguments for the callable
            provide_context=True
        )

    # Get step function input from JSON file

    pod_resources = k8s.V1ResourceRequirements(
            requests={
               "cpu": "16",
               "memory": "64Gi", 
               "ephemeral-storage": "200Gi"
            },  # Request 8 CPU cores, 32GB memory
            limits={
               "cpu": "24",
               "memory": "128Gi",
               "ephemeral-storage": "250Gi"
            },     # Limit to 12 CPU cores, 48GB memory
    )
    
    k = KubernetesPodOperator(
      task_id="test_sar_eddy_docker",
      image="ghcr.io/mike-gangl/podaac-sar-eddy:main",
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
            'OUTPUT_BUCKET_NAME': '{{ var.value.PROCESS_OUTPUTS }}',
            'SEARCH_RESULTS_KEY': "{{ task_instance.xcom_pull(task_ids='asf_search_task', key='search_results') }}",
            'SAR_TASK_ID': '{{ run_id }}',  # Set TASK_ID environment variable
            'AWS_DEFAULT_REGION': 'us-west-2',
            'SSM_EDL_PASSWORD':   "{{params.SSM_EDL_PASSWORD}}",
            'SSM_EDL_USERNAME' : "{{params.SSM_EDL_USERNAME}}"
      },
      container_resources=pod_resources,
      log_events_on_failure=True,
      #cmds=["/bin/sh"],
      #arguments=["-c", "echo hello world"]
      # name="test-error-message",
      # email="airflow@example.com",
      # email_on_failure=True,
    )

    asf_search_task >> k
