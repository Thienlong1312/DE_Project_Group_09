from dagster import RunRequest, sensor

from etl.jobs.vn_jobs_etl_job import run_vn_jobs_etl_job


@sensor(job=run_vn_jobs_etl_job)
def vn_jobs_etl_sensor(_context):
    """
    This is vn_jobs_etl_sensor.
    """
    should_run = True
    if should_run:
        yield RunRequest(run_key=None, run_config={})
