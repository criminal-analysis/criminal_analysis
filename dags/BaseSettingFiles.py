from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
from plugins import gsheet
from plugins import s3

import requests
import logging
import psycopg2
import json


def download_tab_in_gsheet(**context):
    url = context["params"]["url"]
    tab = context["params"]["tab"]
    table = context["params"]["table"]
    data_dir = Variable.get("DATA_DIR")

    gsheet.get_google_sheet_to_csv(
        url,
        tab,
        data_dir + '/' + '{}.csv'.format(table)
    )


def copy_to_s3(**context):
    table = context["params"]["table"]
    s3_key = context["params"]["s3_key"]

    s3_conn_id = "aws_default"
    s3_bucket = "litchiimg"
    data_dir = Variable.get("DATA_DIR")
    local_files_to_upload = [data_dir + '/' + '{}.csv'.format(table)]
    replace = True

    s3.upload_to_s3(s3_conn_id, s3_bucket, s3_key, local_files_to_upload, replace)


dag = DAG(
    dag_id = 'BaseSettingFiles',
    start_date = datetime(2024,5,5), # 날짜가 미래인 경우 실행이 안됨
    schedule = '@once',  # 적당히 조절
    max_active_runs = 1,
    max_active_tasks = 2,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

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
        "tab": "phonenumber_list",
        "schema": "hhee2864",
        "table": "phonenumber_list",
        "table_schema": "local_number varchar(3), sido varchar(6)"
    }
]

with TaskGroup(
    group_id = "BaseSettingFiles",
    tooltip = "Execute before ChildSafetyCenter Dag due to Setup Base Files",
    dag = dag
) as setting_taskgroup:
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
            s3_bucket = "litchiimg",
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


Trigger_MainDag = TriggerDagRunOperator(
    trigger_dag_id = 'ChildSafetyCenter',
    task_id = 'Trigger_MainDag',
    execution_date = '{{ execution_date }}',
    wait_for_completion = True,
    poke_interval = 30,
    reset_dag_run = True,
    dag = dag
)

setting_taskgroup >> Trigger_MainDag
