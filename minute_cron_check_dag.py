"""
Minimal DAG to verify that Airflow is triggering on a one-minute cron schedule.
"""

from datetime import datetime

from airflow.decorators import dag, task


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
    def print_heartbeat():
        print("minute_cron_check: task executed successfully")

    print_heartbeat()


dag = minute_cron_check()
