from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
#docker exec crash_airflow airflow users reset-password -u admin -p admin123
# ================= DEFAULT CONFIG =================
default_args = {
    "owner": "thenmozhi",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ================= DAG =================
with DAG(
    dag_id="crash_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["traffic", "youtube", "spark"],
    description="Urban Traffic Risk Pipeline"
) as dag:

    # ================= BRONZE =================
    # RUN DIRECTLY INSIDE AIRFLOW CONTAINER

    chicago_bronze = BashOperator(
        task_id="chicago_bronze",
        bash_command="""
        python /workspace/data_ingestion/chicago.py
        """
    )

    youtube_bronze = BashOperator(
        task_id="youtube_bronze",
        bash_command="""
        python /workspace/data_ingestion/youtube.py
        """
    )

    # ================= SILVER =================
    # RUN INSIDE SPARK CONTAINER

    chicago_silver = BashOperator(
        task_id="chicago_silver",
        bash_command="docker exec crash_spark_engine spark-submit /workspace/jobs/silver_chicago.py"
    )

    youtube_silver = BashOperator(
        task_id="youtube_silver",
        bash_command="docker exec crash_spark_engine spark-submit /workspace/jobs/silver_youtube.py"
    )

    # ================= GOLD =================

    crash_hotspots = BashOperator(
        task_id="crash_hotspots",
        bash_command="docker exec crash_spark_engine python /workspace/jobs/gold_crash_hotspots.py"
    )

    road_risk_index = BashOperator(
        task_id="road_risk_index",
        bash_command="docker exec crash_spark_engine python /workspace/jobs/road_risk_index.py"
    )

    # ================= DATA WAREHOUSE =================

    data_warehouse = BashOperator(
        task_id="data_warehouse",
        bash_command="docker exec crash_spark_engine python /workspace/jobs/data_warehouse.py"
    )

    # ================= PIPELINE FLOW =================

    chicago_bronze >> chicago_silver >> crash_hotspots
    youtube_bronze >> youtube_silver >> road_risk_index

    [crash_hotspots, road_risk_index] >> data_warehouse