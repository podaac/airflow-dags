from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


TARGET_DAG_ID = "podaac_ecs_cloud_optimized_generator_cold"
CONFIG_FILE = Path(__file__).with_name("cloud_optimize_gen_cold_weekly_runs.json")


def _load_runs() -> list[dict]:
    """Read the weekly run list from the JSON file."""
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_FILE.name}")

    with CONFIG_FILE.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, list):
        raise ValueError("weekly config file must contain a top-level JSON list")

    return payload


with DAG(
    dag_id="podaac_ecs_cloud_optimized_generator_cold_weekly_controller",
    description="Weekly controller that triggers the cold generator DAG from a JSON list",
    start_date=datetime(2021, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["aws", "ecs", "cloud-optimized", "controller", "weekly"],
) as dag:

    @task
    def build_trigger_kwargs() -> list[dict]:
        from airflow.operators.python import get_current_context

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

    TriggerDagRunOperator.partial(
        task_id="trigger_cold_generator",
        wait_for_completion=False,
    ).expand_kwargs(build_trigger_kwargs())
