from airflow import DAG
from airflow.decorators import task

from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta

import csv
import json
import logging
import psycopg2
import requests
from requests.exceptions import HTTPError
from http import HTTPStatus

import os
import pandas as pd
import time
from collections import defaultdict

from plugins import gsheet
from plugins import s3
from plugins import update_sido


s3_conn_id = "aws_default"
s3_bucket = "litchiimg"
data_dir = Variable.get("DATA_DIR")

sheets = [
    {
        "url": f"{Variable.get('gsheet_url_zip')}",
        "tab": "zipcode_new",
        "schema": "hhee2864",
        "table": "zipcode_new",
        "table_schema": "sido varchar(6), zip_code varchar(50)"
    },
    {
        "url": f"{Variable.get('gsheet_url_zip')}",
        "tab": "zipcode_old",
        "schema": "hhee2864",
        "table": "zipcode_old",
        "table_schema": "sido varchar(6), zip_code varchar(50)"
    },
    {
        "url": f"{Variable.get('gsheet_url_name_list')}",
        "tab": "localname_list",
        "schema": "hhee2864",
        "table": "localname_list",
        "table_schema": "local_name varchar(24), sido varchar(6)"
    },
    {
        "url": f"{Variable.get('gsheet_url_name_list')}",
        "tab": "localname_iso_list",
        "schema": "hhee2864",
        "table": "localname_iso_list",
        "table_schema": "sido varchar(6), iso_code varchar(10)"
    },
]

url = "https://www.safe182.go.kr/api/lcm/safeMap.do"
schema = 'hhee2864'
table_info = {
    "table_name": "child_safety_center",
    "table_schema": [
        "serial_no int primary key",
        "place_name varchar(150) not null",
        "sido varchar(30) not null",
        "sigungu varchar(30)",
        "address varchar(1000)",
        "zip_code varchar(10)",
        "phone_number varchar(20)"
    ]
}

def download_tab_in_gsheet(**context):
    url = context["params"]["url"]
    tab = context["params"]["tab"]
    table = context["params"]["table"]

    gsheet.get_google_sheet_to_csv(
        url,
        tab,
        data_dir + '/' + '{}.csv'.format(table)
    )


def copy_to_s3(**context):
    table = context["params"]["table"]
    s3_key = context["params"]["s3_key"]

    local_files_to_upload = [data_dir + '/' + '{}.csv'.format(table)]
    replace = True

    s3.upload_to_s3(s3_conn_id, s3_bucket, s3_key, local_files_to_upload, replace)


def get_raw_data_count(url, params, is_local):
    if is_local:
        raw_count = update_sido.get_local_rows_count()
    else:
        first_info = requests.post(url, params=params)
        if first_info.status_code != HTTPStatus.OK:
            raise first_info.raise_for_status()
        
        first_data = first_info.json()
        raw_count = first_data["totalCount"]

    return raw_count


def save_raw_data(data, path):
    df = pd.json_normalize(data)
    path = path + "/" + "raw_safety_center_list.csv"
    result = df.to_csv(path, index=False, encoding="UTF-8", quoting=csv.QUOTE_ALL, header=True)

    return path

def download_raw_data(s3_conn_id, s3_bucket, s3_key, path):
    download_path = s3.download_from_s3(s3_conn_id, s3_bucket, s3_key, path)
    origin_path = path + "/" + "raw_safety_center_list.csv"

    if not download_path:
        if os.path.exists(origin_path):
            if os.stat(origin_path).st_mtime < time.time() - 43200:
                os.remove(origin_path)
                download_path = None
            else:
                download_path = origin_path
    
    return download_path

def update_sido_data(path):
    raw_safety_center_df, local_name_df, zip_code_new_df, zip_code_old_df = update_sido.load_data(path)

    raw_safety_center_df = update_sido.preprocess_data(raw_safety_center_df)
    raw_safety_center_df = update_sido.extract_city(raw_safety_center_df)
    
    merged_df = update_sido.join_local_name(raw_safety_center_df, local_name_df)
    zip_code_new_df, zip_code_old_df = update_sido.preprocess_zip_codes(zip_code_new_df, zip_code_old_df)

    merged_df = update_sido.update_sido_zip(merged_df, zip_code_new_df, zip_code_old_df)
    merged_df = update_sido.update_sido_local_name(merged_df)
    logging.info(f"sido parsing counts: {merged_df[~merged_df['sido'].isna()].count()}")

    merged_df = update_sido.fill_missing_sido(merged_df)
    logging.info(f"missing counts: {merged_df[merged_df['sido'].isna()].count()}")

    path = update_sido.save_to_csv(merged_df, data_dir + '/' + 'updated_safety_center_list.csv')
    return (path, merged_df["serial_no"].count())

@task
def extract(url):
    logging.info(datetime.now())

    params = {
        "esntlId": f"{Variable.get('police_api_id')}",
        "authKey": f"{Variable.get('police_api_key')}",
        "pageIndex": "1",
        "pageUnit": "1",
        "detailDate1": "09",
        "xmlUseYN": "N"
    }

    try:
        s3_key = "upload" + "/" + "raw_safety_center_list.csv"
        path = download_raw_data(s3_conn_id, s3_bucket, s3_key, data_dir)

        total_count = get_raw_data_count(url, params, True)
        remote_total_count = get_raw_data_count(url, params, False)

        if not path or total_count != remote_total_count:
            center_info = defaultdict(list)
            loopCount = remote_total_count // 100 + 1
            logging.info(f"loopCount: {loopCount}")
            params["pageUnit"] = 100
            
            for idx in range(1, loopCount + 1):
                time.sleep(1)
                logging.info(f"idx >> {idx}")
                params["pageIndex"] = idx
                data = requests.post(url, params=params)
                data = data.json()["list"]
                center_info["list"].extend(data)
            
            path = save_raw_data(center_info["list"], data_dir)
            total_count = get_raw_data_count(url, params, True)

            s3.upload_to_s3(s3_conn_id, s3_bucket, s3_key, [path], True)

    except (HTTPError, Exception) as e:
        logging.error(e)
        raise

    return (path, total_count)

@task
def transform(input_value):
    logging.info("Transform started")

    path, total_count = input_value
    records = []

    csv_path, transform_count = update_sido_data(path)
    if total_count != transform_count:
        logging.error(f"raw data count {total_count} and transform data count {transform_count} is not same.")
        raise ValueError

    s3_key = "upload" + "/" + "updated_safety_center_list.csv"
    s3.upload_to_s3(s3_conn_id, s3_bucket, s3_key, [csv_path], True)
    
    logging.info("Transform ended")


with DAG(
    dag_id='ChildSafetyCenter',
    start_date=datetime(2024,5,25),
    schedule = '@once',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    },
    template_searchpath=[f"{Variable.get('INCLUDE_DIR')}"],
) as dag:

    with TaskGroup(
        group_id = "BaseSettingTaskGroup",
        tooltip = "Execute before Main Tasks due to base setting",
        dag = dag
    ) as base_setting_taskgroup:
        for sheet in sheets:
            table_setting_task = SQLExecuteQueryOperator(
                task_id = 'table_setting_{}_in_redshfit'.format(sheet["table"]),
                conn_id = "redshift_dev_db",
                sql = f"""
                DROP TABLE IF EXISTS {sheet["table"]};
                CREATE TABLE {sheet["table"]} ({sheet["table_schema"]});
                """,
                autocommit = True,
                split_statements = True,
                return_last = False,
                dag = dag
            )

            download_task = PythonOperator(
                task_id = 'download_{}_in_gsheet'.format(sheet["table"]),
                python_callable = download_tab_in_gsheet,
                params = sheet,
                dag = dag)

            s3_key = "upload" + "/" + sheet["schema"] + "_" + sheet["table"] + ".csv"

            copy_task = PythonOperator(
                task_id = 'copy_{}_to_s3'.format(sheet["table"]),
                python_callable = copy_to_s3,
                params = {
                    "table": sheet["table"],
                    "s3_key": s3_key
                },
                dag = dag)

            run_copy_sql = S3ToRedshiftOperator(
                task_id = 'run_copy_sql_{}'.format(sheet["table"]),
                s3_bucket = s3_bucket,
                s3_key = s3_key,
                schema = sheet["schema"],
                table = sheet["table"],
                copy_options = ["csv", "IGNOREHEADER AS 1", "QUOTE AS '\"'", "DELIMITER ','"],
                method = 'REPLACE',
                redshift_conn_id = "redshift_dev_db",
                aws_conn_id = 'aws_default',
                dag = dag
            )

            table_setting_task >> download_task >> copy_task >> run_copy_sql

    with TaskGroup(
        group_id = "MainTaskGroup",
        tooltip = "Police 182 API data ETL and ELT",
        dag = dag
    ) as main_taskgroup:
        table_setting_task = SQLExecuteQueryOperator(
            task_id = 'table_setting_{}_in_redshfit'.format(table_info["table_name"]),
            conn_id = "redshift_dev_db",
            sql = f"""
            DROP TABLE IF EXISTS {table_info["table_name"]};
            CREATE TABLE {table_info["table_name"]} ({",".join(table_info["table_schema"])});""",
            autocommit = True,
            split_statements = True,
            return_last = False,
            dag = dag
        )

        load_task = S3ToRedshiftOperator(
            task_id = 'run_copy_sql_{}'.format(table_info["table_name"]),
            s3_bucket = s3_bucket,
            s3_key = "upload" + "/" + "updated_safety_center_list.csv",
            schema = schema,
            table = table_info["table_name"],
            column_list = ["serial_no", "place_name", "phone_number", "address", "zip_code", "sigungu", "sido"],
            copy_options = ["csv", "IGNOREHEADER AS 1", "QUOTE AS '\"'", "DELIMITER ','"],
            method = 'REPLACE',
            redshift_conn_id = "redshift_dev_db",
            aws_conn_id = 'aws_default',
            dag = dag
        )

        elt_task = SQLExecuteQueryOperator(
            task_id = 'elt_task',
            conn_id = "redshift_dev_db",
            sql = "child_safety_center_summary.sql",
            autocommit = True,
            split_statements = True,
            return_last = False,
            dag = dag
        )

        extract_transform_task = transform(extract(url))
        extract_transform_task >> table_setting_task >> load_task >> elt_task

    base_setting_taskgroup >> main_taskgroup
