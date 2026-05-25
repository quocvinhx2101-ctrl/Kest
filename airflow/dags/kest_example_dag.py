from datetime import datetime

from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "kest",
    "retries": 1,
}

@dag(
    dag_id="kest_example_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
)
def kest_example_pipeline():
    @task.bash
    def run_dlt():
        return "bash -lc 'bash /opt/kest/scripts/run_dlt.sh'"

    @task.bash
    def run_bronze_validation():
        return "bash -lc 'bash /opt/kest/scripts/run_soda.sh bronze'"

    @task.bash
    def run_silver():
        return "bash -lc 'bash /opt/kest/scripts/run_duckdb_stage.sh silver'"

    @task.bash
    def run_silver_checks():
        return "bash -lc 'bash /opt/kest/scripts/run_soda.sh silver'"

    @task.bash
    def run_gold():
        return "bash -lc 'bash /opt/kest/scripts/run_duckdb_stage.sh gold'"

    @task.bash
    def run_gold_checks():
        return "bash -lc 'bash /opt/kest/scripts/run_soda.sh gold'"

    @task.bash
    def refresh_serving_views():
        return "bash -lc 'bash /opt/kest/scripts/run_duckdb_stage.sh serving'"

    run_dlt() >> run_bronze_validation() >> run_silver() >> run_silver_checks() >> run_gold() >> run_gold_checks() >> refresh_serving_views()


kest_example_pipeline()
