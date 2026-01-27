"""
DAG to print out all environment variables of the Airflow system.
Uses the modern TaskFlow API with @dag and @task decorators.
"""
import os
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="print_airflow_environment",
    description="Prints all environment variables available to the Airflow system",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["utility", "debug", "environment"],
)
def print_airflow_environment():
    """DAG that prints the current environment variables of the Airflow system."""

    @task()
    def print_environment_variables():
        """Task to retrieve and print all environment variables."""
        env_vars = dict(os.environ)
        
        print("=" * 80)
        print("AIRFLOW SYSTEM ENVIRONMENT VARIABLES")
        print("=" * 80)
        print(f"Total environment variables: {len(env_vars)}")
        print("=" * 80)
        
        # Sort and print all environment variables
        for key in sorted(env_vars.keys()):
            value = env_vars[key]
            # Mask potentially sensitive values
            if any(sensitive in key.upper() for sensitive in ["PASSWORD", "SECRET", "TOKEN", "KEY", "CREDENTIAL"]):
                value = "***MASKED***"
            print(f"{key}={value}")
        
        print("=" * 80)
        print("END OF ENVIRONMENT VARIABLES")
        print("=" * 80)
        
        return {"total_variables": len(env_vars), "variable_names": list(sorted(env_vars.keys()))}

    print_environment_variables()


dag = print_airflow_environment()
