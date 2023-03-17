from etl.jobs.vn_jobs_etl_job import run_vn_jobs_etl_job


def test_run_vn_jobs_etl():
    """
    This is an example test for a Dagster job.

    For hints on how to test your Dagster graphs, see our documentation tutorial on Testing:
    https://docs.dagster.io/concepts/testing
    """

    result = run_vn_jobs_etl_job.execute_in_process()

    assert result.success
    assert len(result.output_for_node("extract_from_scrape_bot"))>0
    assert len(result.output_for_node("extract_from_database"))>0
    assert len(result.output_for_node("transform_raw_data"))>0
    assert result.output_for_node("load_to_database") == "done"