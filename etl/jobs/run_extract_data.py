from dagster import job

from etl.ops.extract_data import extract_from_api

@job
def run_extract_data_job():

    extract_from_api()
