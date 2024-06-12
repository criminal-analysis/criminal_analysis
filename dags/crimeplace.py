'''
* KOSIS API 사용 DAG 작성*
+ 테스크 수정 : api 호출, s3 저장 >> 주소 뽑아내기 >> redshift 테이블 적재 
+ Airflow Connection에 등록된 aws 연결 정보를 가져옴
+ S3 연결 함수 구현
'''


from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from datetime import datetime
import boto3

import logging
import requests
import json

# Airflow Connection에 등록된 Redshift 연결 정보를 가져옴
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# Airflow Connection에 등록된 S3 연결 정보를 가져옴
def s3_connection():
    aws_conn_id = 'aws_default'
    aws_hook = AwsBaseHook(aws_conn_id)
    credentials = aws_hook.get_credentials()
    s3 = boto3.client('s3', aws_access_key_id=credentials.access_key, aws_secret_access_key=credentials.secret_key)
    return s3



# 1. API 호출 및 S3에 저장하는 함수
@task
def api_save_to_s3():
    # API 호출
    response = requests.get('https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey=MGU3MGE4ZWU1YjYzNmJiNjFjMTEzODNkMGE0YmZiYTI=&itmId=00+&objL1=00+01+0101+0102+0103+0104+0106+0107+0108+0109+0105+02+03+0301+0302+0303+0304+0305+0306+0307+0308+07+10+&objL2=ALL&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&newEstPrdCnt=10&orgId=132&tblId=DT_13204_3106')
    data = response.json()

    # Boto3 클라이언트를 생성 -> S3에 저장
    s3 = s3_connection()
    bucket_name = 'litchiimg'
    file_name = 'crime_place_data.json'

    with open(file_name, 'w') as f:
        json.dump(data, f)

    s3.upload_file(file_name, bucket_name, file_name)
    logging.info("s3 upload done")


# 2. S3에서 JSON 파일을 가져오고 파싱, 전처리
@task
def get_and_parsing():
    s3 = s3_connection()
    bucket_name = 'litchiimg'
    file_name = 'crime_place_data.json'

    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    crimes = obj['Body'].read().decode('utf-8')

    records = []
    for crime in json.loads(crimes):
        occured_date = crime["PRD_DE"]
        subcategory_code = crime["C1"]
        if subcategory_code in ['00', '01', '03']:
            continue
        category = {"01": "강력범죄", "02" : "절도범죄","03" : "폭력범죄", "07": "마약범죄", "10" : "교통범죄"}.get(subcategory_code[0:2])
        subcategory = crime["C1_NM"]
        place = crime["C2_NM"]
        if place == '계':
            continue
        place_cnt0 = crime["DT"]
        place_cnt = place_cnt0.replace("-", "0")
        records.append([occured_date, subcategory_code, category, subcategory, place, place_cnt])
    logging.info("parsing done")
    return records



# 3. 테이블 형식으로 Redshift 적재
@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
            CREATE TABLE {schema}.{table} (
                occured_date int,
                category_code int,
                category  VARCHAR(20),
                subcategory VARCHAR(30),
                place  VARCHAR(30),
                place_cnt int
            );""")
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ({r[0]}, {r[1]}, '{r[2]}', '{r[3]}', '{r[4]}', {r[5]});"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("load done")



with DAG(
    dag_id = 'CrimePlace',
    start_date = datetime(2023,1,1),
    catchup=False,
    tags=['API'],
    schedule = '@yearly' # 	1년에 한번씩 1월 1일 자정에 실행
) as dag:
    api_task = api_save_to_s3()
    parsing_task = get_and_parsing()
    load_task = load("dowon0215", "crime_place", parsing_task)

    api_task >> parsing_task >> load_task