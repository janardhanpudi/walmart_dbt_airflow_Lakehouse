from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import time
ws = WorkspaceClient(
    host="https://dbc-6edd4509-d681.cloud.databricks.com",
    # Create or manage tokens in Databricks: User Settings > Developer > Access tokens.
    token="PASTE_NEW_DATABRICKS_TOKEN_HERE"
)

job_trigger = ws.jobs.run_now(job_id=490434197775662)



while True:

    job_run = ws.jobs.get_run(run_id=job_trigger.run_id)

    print(f"Job run state: {job_run.state.life_cycle_state}, result state: {job_run.state.result_state}")

    if job_run.state.life_cycle_state in [ RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
        if job_run.state.result_state == RunResultState.SUCCESS:
            print("finished successfully.") 
            break
        else:
            raise Exception(f"Job run failed with state: {job_run.state.result_state}")

    time.sleep(5) # Wait for 5 seconds before checking the job status again
