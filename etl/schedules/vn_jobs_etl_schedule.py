from dagster import schedule

from etl.jobs.vn_jobs_etl_job import run_vn_jobs_etl_job


@schedule(cron_schedule="0 10 * * *", job=run_vn_jobs_etl_job, execution_timezone="US/Central")
def vn_jobs_etl_job_schedule(_context):
    run_config = {}
    return run_config
