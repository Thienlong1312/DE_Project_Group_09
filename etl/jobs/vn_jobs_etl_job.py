from dagster import job

from etl.ops.vn_jobs_etl import extract_from_scrape_bot, extract_from_database,transform_raw_data, load_to_database, load_to_s3

@job
def run_vn_jobs_etl_job():

    df = extract_from_scrape_bot()
    df_database = extract_from_database()
    df_dictionary = transform_raw_data(df, df_database)
    load_to_database(df_dictionary)
    load_to_s3(df, df_dictionary)
    # visualize()
    # load_data(df, bunch_of_df)
