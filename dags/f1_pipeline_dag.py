from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import os

# ---------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------
DB_PATH = "/opt/airflow/data/f1_data.db"
SCRIPTS_PATH = "/opt/airflow/scripts"


# ---------------------------------------------------------------------
# Helper: Determine which ETL to run
# ---------------------------------------------------------------------
def decide_etl_mode(**context):
    exists = os.path.exists(DB_PATH)
    size = os.path.getsize(DB_PATH) if exists else 0

    print("DEBUG: Checking DB:", DB_PATH)
    print("DEBUG: exists:", exists)
    print("DEBUG: size:", size)

    if exists and size > 0:
        print("DEBUG: Branch → run_update_etl")
        return "run_update_etl"
    else:
        print("DEBUG: Branch → run_initial_etl")
        return "run_initial_etl"


# ---------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------
default_args = {
    "owner": "group_3",
    "start_date": days_ago(1),
    "retries": 1,
}


# ---------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------
with DAG(
    dag_id="f1_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # --------------------------------------------------------------
    # 1. Branch Decision
    # --------------------------------------------------------------
    choose_etl_mode = BranchPythonOperator(
        task_id="choose_etl_mode",
        python_callable=decide_etl_mode,
    )

    # --------------------------------------------------------------
    # 2. Initial full ETL (only runs if DB does not exist)
    # --------------------------------------------------------------
    run_initial_etl = BashOperator(
        task_id="run_initial_etl",
        bash_command=f"python3 {SCRIPTS_PATH}/load_f1_functional.py",
    )

    # --------------------------------------------------------------
    # 3. Incremental ETL update (only runs if DB exists)
    # --------------------------------------------------------------
    run_update_etl = BashOperator(
        task_id="run_update_etl",
        bash_command=f"python3 {SCRIPTS_PATH}/update_f1_data.py",
    )

    # --------------------------------------------------------------
    # 4. Compute tyre strategy data (runs after either ETL)
    # --------------------------------------------------------------
    compute_tyre_changes = BashOperator(
        task_id="compute_tyre_changes",
        bash_command=f"python3 {SCRIPTS_PATH}/create_tyre_changes.py",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # --------------------------------------------------------------
    # 5. Final processing (app logic)
    # --------------------------------------------------------------
    run_app = BashOperator(
        task_id="run_app",
            bash_command=f"streamlit run {SCRIPTS_PATH}/app.py --server.address=0.0.0.0 --server.port=8501 &",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # --------------------------------------------------------------
    # DAG FLOW
    # --------------------------------------------------------------
    choose_etl_mode >> [run_initial_etl, run_update_etl]

    run_initial_etl >> compute_tyre_changes
    run_update_etl >> compute_tyre_changes

    compute_tyre_changes >> run_app
