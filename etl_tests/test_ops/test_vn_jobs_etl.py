from etl.ops.vn_jobs_etl import extract_from_scrape_bot, extract_from_database, transform_raw_data, load_to_database


def test_extract_from_scrapebot():
    df = extract_from_scrape_bot()
    assert len(df)>0

def test_extract_from_database():
    df_database = extract_from_database()
    assert len(df_database)>0

def test_transform_raw_data():

    df = extract_from_scrape_bot()
    df_database = extract_from_database()
    df_dictionary = transform_raw_data(df, df_database)
    assert len(df_dictionary)>0

def test_load_to_database():
    assert load_to_database() == "done"