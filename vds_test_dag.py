"""Airflow 3.2.1 DAG to run VDS tests for each collection/protocol combination."""

from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonVirtualenvOperator

ALL_COLLECTIONS = [
    "MUR25-JPL-L4-GLOB-v04.2",
    "TELLUS_GRAC-GRFO_MASCON_CRI_GRID_RL06.3_V4",
    "CCMP_WINDS_10M6HR_L4_V3.1",
    "SWOT_L2_LR_SSH_Basic_D",
]

DAG_DIR = Path(__file__).parent
REQUIREMENTS = Path(__file__).with_name("requirements.txt").read_text().splitlines()


def run_vds_test(collection: str, dag_dir: str, earthdata_username: str, earthdata_password: str):
    import subprocess
    import sys
    import os as _os

    env = _os.environ.copy()
    env["EARTHDATA_USERNAME"] = earthdata_username
    env["EARTHDATA_PASSWORD"] = earthdata_password

    result = subprocess.run(
        [sys.executable, _os.path.join(dag_dir, "vds_test.py"), collection],
        capture_output=True,
        text=True,
        cwd=dag_dir,
        env=env,
    )

    print(result.stdout)
    if result.stderr:
        print(result.stderr)

    if result.returncode != 0 or "FAILED" in result.stdout:
        raise Exception(f"VDS test failed for {collection}")

    return {"collection": collection, "passed": True}


with DAG(
    dag_id="vds_integration_tests",
    description="Run VDS virtual collection tests across collections and protocols",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["vds", "testing", "podaac"],
    params={
        "collections": ALL_COLLECTIONS,
    },
) as dag:

    @task
    def get_collections(**context):
        return context["params"]["collections"]

    collections = get_collections()

    run_tests = PythonVirtualenvOperator.partial(
        task_id="run_vds_test",
        python_callable=run_vds_test,
        requirements=REQUIREMENTS,
        system_site_packages=False,
        op_kwargs={
            "dag_dir": str(DAG_DIR),
            "earthdata_username": "{{ var.value.EARTHDATA_USERNAME }}",
            "earthdata_password": "{{ var.value.EARTHDATA_PASSWORD }}",
        },
    ).expand(op_args=collections.map(lambda col: [col]))
