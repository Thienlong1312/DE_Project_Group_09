from dagster import repository

from etl.jobs.say_hello import say_hello_job

from etl.jobs.vn_jobs_etl_job import run_vn_jobs_etl_job


# from etl.jobs.run_etl import run_etl_job
#etl job schedule
from etl.schedules.vn_jobs_etl_schedule import vn_jobs_etl_job_schedule
from etl.sensors.my_sensor import my_sensor
from etl.sensors.vn_jobs_etl_sensor import vn_jobs_etl_sensor


@repository
def etl():
    """
    The repository definition for this etl Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [run_vn_jobs_etl_job]
    schedules = [vn_jobs_etl_job_schedule]
    sensors = [vn_jobs_etl_sensor]

    return jobs + schedules + sensors
