from databricks.sdk import WorkspaceClient
import time
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
import pendulum

@dag(
    dag_id="orchestrate",
    schedule="0 16 * * *",
    catchup=False,
    start_date=pendulum.datetime(year=2026, month=10, day=13, tz='Asia/Kolkata'),
)

def orchestrate():

    @task
    def ingest_cdc():
        ws = WorkspaceClient(
            host="https://dbc-6edd4509-d681.cloud.databricks.com",
            # Create or manage tokens in Databricks: User Settings > Developer > Access tokens.
            token="PASTE_NEW_DATABRICKS_TOKEN_HERE"
        )

        job_trigger = ws.jobs.run_now(job_id=490434197775662)



        while True:

            job_run = ws.jobs.get_run(run_id=job_trigger.run_id)

            if job_run.state.life_cycle_state in [ RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("finished successfully.") 
                    break
                else:
                    raise Exception(f"Job run failed with state: {job_run.state.result_state}")

            time.sleep(5) # Wait for 5 seconds before checking the job status again
        return "CDC data Ingested"

    @task.bash
    def clean_up():
        return "rm -rf /opt/airflow/walmart_dbt/walmart_dp/target && rm -rf /opt/airflow/walmart_dbt/walmart_dp/logs && rm -rf /opt/airflow/walmart_dbt/logs"

    @task.bash
    def source_freshness():
        #Manually set working directory using cd command
        return "cd /opt/airflow/walmart_dbt/walmart_dp && dbt source freshness --profiles-dir /opt/airflow/walmart_dbt"

    silver_technical = BashOperator(
        task_id='silver_technical',
        cwd='/opt/airflow/walmart_dbt/walmart_dp',
        bash_command='dbt run --profiles-dir /opt/airflow/walmart_dbt --select silver_t'
    )

    silver_technical_tests = BashOperator(
        task_id='silver_technical_tests',
        cwd='/opt/airflow/walmart_dbt/walmart_dp',
        bash_command='dbt test --profiles-dir /opt/airflow/walmart_dbt --select silver_t'
    )

    silver_business = BashOperator(
        task_id='silver_business',
        cwd='/opt/airflow/walmart_dbt/walmart_dp',
        bash_command='dbt run --profiles-dir /opt/airflow/walmart_dbt --select silver_b'
    )


    silver_business_tests = BashOperator(
        task_id='silver_business_tests',
        cwd='/opt/airflow/walmart_dbt/walmart_dp',
        bash_command='dbt test --profiles-dir /opt/airflow/walmart_dbt --select silver_b'
    )

    gold_ephemeral = BashOperator(
        task_id='gold_ephemeral',
        cwd='/opt/airflow/walmart_dbt/walmart_dp',
        bash_command='dbt run --profiles-dir /opt/airflow/walmart_dbt --select models/gold/ephemeral'
    )

    gold_dimensions = BashOperator(
        task_id='gold_dimensions',
        cwd='/opt/airflow/walmart_dbt/walmart_dp',
        bash_command='dbt snapshot --profiles-dir /opt/airflow/walmart_dbt'
    )

    gold_facts = BashOperator(
        task_id='gold_facts',
        cwd='/opt/airflow/walmart_dbt/walmart_dp',
        bash_command='dbt run --profiles-dir /opt/airflow/walmart_dbt --select models/gold/fact                                                                                                                                                                                                                                                                                                                                                                             '
    )


    ingest_cdc() >> clean_up() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_facts
 

orchestrate_dag = orchestrate()