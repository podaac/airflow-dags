"""
Minimal DAG to verify that Airflow is triggering on a one-minute cron schedule.
"""

import json
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task


LAUNCHES_FILE = Path(__file__).with_name("launches.json")


@dag(
    dag_id="minute_cron_check",
    description="Runs every minute and prints a message for cron verification",
    start_date=datetime(2025, 1, 1),
    schedule="* * * * *",
    catchup=False,
    tags=["debug", "cron", "minute"],
)
def minute_cron_check():
    """Very small DAG for confirming Airflow minute-level scheduling."""

    @task
    def print_launches_config():
        if not LAUNCHES_FILE.exists():
            print(f"minute_cron_check: {LAUNCHES_FILE.name} not found")
            return {"found": False, "launch_count": 0}

        with LAUNCHES_FILE.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)

        launches = payload.get("launches", [])
        print(f"minute_cron_check: loaded {len(launches)} launches from {LAUNCHES_FILE.name}")
        print(json.dumps(payload, indent=2, sort_keys=True))

        return {"found": True, "launch_count": len(launches)}

    print_launches_config()


dag = minute_cron_check()
