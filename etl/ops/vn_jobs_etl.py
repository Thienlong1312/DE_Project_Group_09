from dagster import Out, Output, job, op
import logging
from etl.db_con import get_sql_conn, get_postgres_creds

#import needed libraries
from sqlalchemy import create_engine
import pyodbc

import io
import os
import sys
from datetime import date, datetime
from datetime import timedelta
import pandas as pd

import boto3

from etl.ops.utils.transform_data import DataTransformer
from etl.ops.utils.postgresql_information import POSTGRES_HOST, POSTGRES_HOST_AUTH_METHOD, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER, POSTGRES_DB

#extract data from sql server

@op(out={"df": Out(is_required=True)})
def extract_from_scrape_bot():
    try:
        today = date.today()
        current_time = str(datetime.now().strftime("%H%M"))
        yesterday = today - timedelta(days = 1)
        yesterday = str(yesterday).replace('-', '')
        # os.system('cmd cd ./etl/ops/')
        file_name = "all_jobs_cb_"+yesterday+current_time+".csv"
        command = "scrapy crawl careerbuilder -o " + file_name
        # os.system('cmd /c "cd ./etl/ops/ & '+command+'"')
        
        # df = pd.read_csv("./etl/ops/"+file_name)
        df = pd.read_csv("./etl/ops/all_jobs_cb_202302251233.csv")
        print(df.head())

        return Output(df, "df")
    except Exception as err:
        print("Extracting corrupted.")
        print(err)

@op(out={"df_database": Out(is_required=True)})
def extract_from_database():
    user = POSTGRES_USER
    password = POSTGRES_PASSWORD
    host = "localhost"
    port = POSTGRES_PORT
    dbname = POSTGRES_DB
    connection_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    psql_engine = create_engine(connection_str)


    sql = """
    SELECT company_title, company_id
    FROM companies
    """
    df_companies_sv = pd.read_sql_query(sql, psql_engine)

    print(df_companies_sv)
    print()

    print("Number of company_id unique:", df_companies_sv['company_id'].unique().size, "unique values")
    print("Check number of company_id unique:")
    if len(df_companies_sv) == df_companies_sv['company_id'].unique().size:
        print(True)
    else:
        raise Exception("Number of company_id unique does not equal to total number of rows of df_companies_sv")
    print()

    print("Number of company_title unique:", df_companies_sv['company_title'].unique().size, "unique values")
    print("Check number of company_title unique:")
    if len(df_companies_sv) == df_companies_sv['company_title'].unique().size:
        print(True)
    else:
        raise Exception("Number of company_title unique does not equal to total number of rows of df_companies_sv")
    print()

    print("Query df_job_id from server:")

    sql = """
    SELECT job_id
    FROM jobs
    """
    df_job_id = pd.read_sql_query(sql, psql_engine)

    print(df_job_id)
    print()

    print("Number of job_id unique:", df_job_id['job_id'].unique().size, "unique values")
    print("Check number of job_id unique:")
    if len(df_job_id) == df_job_id['job_id'].unique().size:
        print(True)
    else:
        raise Exception("Number of job_id unique does not equal to total number of rows of df_job_id")
    print()

    print("Query df_ordered_cap_bac from server:")
    sql = """
    SELECT *
    FROM ordered_cap_bac
    """
    df_ordered_cap_bac = pd.read_sql_query(sql, psql_engine)

    print(df_ordered_cap_bac)
    print()

    cap_bac = list(filter(lambda x: x!="Unknown", df_ordered_cap_bac['cap_bac'].unique()))

    print("Query city from server:")
    
    sql = """
    SELECT *
    FROM city_country
    """
    df_city = pd.read_sql_query(sql, psql_engine)

    df_city.drop(['Country'], axis=1)
    print(df_city)
    print()

    print("Number of city unique:", df_city['City'].unique().size, "unique values")
    print("Check number of city unique:")
    if len(df_city) == df_city['City'].unique().size:
        print(True)
    else:
        raise Exception("Number of city unique does not equal to total number of rows of df_city")
    print()

    df_database = {"city_country": df_city,
    "companies": df_companies_sv,
    "jobs": df_job_id,
    "cap_bac": cap_bac,
    "ordered_cap_bac": df_ordered_cap_bac,
    }

    return df_database

@op(out={"df_dictionary": Out(is_required=True)})
def transform_raw_data(df, df_database):
    try:
        transformer = DataTransformer(df, df_database)
        return transformer.processing_raw_data()
    except Exception as err:
        print("Transforming corrupted.")
        print(err)

@op
def load_to_database(df_dictionary):
    try:
        user = POSTGRES_USER
        password = POSTGRES_PASSWORD
        host = "localhost"
        port = POSTGRES_PORT
        dbname = POSTGRES_DB
        connection_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        psql_engine = create_engine(connection_str)

        for key in df_dictionary.keys():
            df_dictionary.get(key).to_sql(
                key,
                psql_engine,
                if_exists = "replace",
                index = False,
                chunksize = 10000,
                method = "multi"
            )
        return "done"
    except Exception as err:
        print("Loading to database corrupted.")
        print(err)

@op
def load_to_s3(df, df_dictionary):
    try:
        AWS_ACCESS_KEY_ID = ""
        AWS_SECRET_ACCESS_KEY = ""
        DATALAKE_BUCKET = "vnjobs"

        today = date.today()
        current_time = str(datetime.now().strftime("%H%M"))
        # yesterday = today - timedelta(days = 1)
        today = str(today).replace('-', '')

        raw_name = "all_jobs_cb_202302251233"

        mother_path = today + current_time + "/"
        filepath = mother_path + "raw/" + raw_name + ".csv"

        s3_cilent = boto3.client("s3", aws_access_key_id = AWS_ACCESS_KEY_ID,
                                aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
                                region_name = "ap-southeast-1")
        
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)

            response = s3_cilent.put_object(
                Bucket = DATALAKE_BUCKET, Key = filepath, Body=csv_buffer.getvalue()
            )
        
        for key in df_dictionary.keys():
            print("key", key)
            # if key == "cap_bac":
                # continue
            cleansed_name = key
            filepath = mother_path + "cleansed/" + cleansed_name + ".csv"
            
            with io.StringIO() as csv_buffer:
                df_dictionary.get(key).to_csv(csv_buffer, index=False)

                response = s3_cilent.put_object(
                    Bucket = DATALAKE_BUCKET, Key = filepath, Body=csv_buffer.getvalue()
                )

        print("Data imported successfully.")
    except Exception as err:
        print("Data load error."+str(err))