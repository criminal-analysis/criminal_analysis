from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import boto3
import json

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def api_get_upload_s3(url):
    result = requests.get(url).text
    json_object = json.loads(result)
    file_name = 'crime_region_raw_data.json'

    with open(file_name, 'w', encoding="UTF-8") as f:
        json.dump(json_object, f, ensure_ascii=False)

    ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
    SECRET_KEY = Variable.get("AWS_SECRET_KEY")
    bucket_name = "litchiimg"
    key = "crime_region_raw_data.json"

    s3_client = boto3.client(
        service_name='s3',
        region_name="ap-northeast-2",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    s3_client.upload_file(file_name, bucket_name, key)
    logging.info("s3 upload done")


@task
def extract():
    ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
    SECRET_KEY = Variable.get("AWS_SECRET_KEY")
    
    # s3 클라이언트 연결
    s3_client = boto3.client(
        service_name='s3',
        region_name="ap-northeast-2",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    bucket_name = "litchiimg"
    file_name = 'crime_region_raw_data.json'
    res = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    content = res['Body'].read().decode('utf-8')
    json_obj = json.loads(content)
    
    return json_obj


@task
def transform(obj):
    records = []
    category_dict = {"01": "강력범죄", "02": "절도범죄", "03": "폭력범죄", "07": "마약범죄", "10": "교통범죄"}
    do_dict = {
        "수원시": "경기", "고양시": "경기", "용인시": "경기", "과천시": "경기", "광명시": "경기", "광주시": "경기", "구리시": "경기", "군포시": "경기", "김포시": "경기", "남양주시": "경기", "동두천시": "경기", "부천시": "경기", "성남시": "경기", "시흥시": "경기", "안산시": "경기", "안성시": "경기", "안양시": "경기", "양주시": "경기", "여주시": "경기", "오산시": "경기", "의왕시": "경기", "의정부시": "경기", "이천시": "경기", "파주시": "경기", "평택시": "경기", "포천시": "경기", "하남시": "경기", "화성시": "경기", # 경기
        "강릉시": "강원", "동해시": "강원", "삼척시": "강원", "속초시": "강원", "원주시": "강원", "춘천시": "강원", "태백시": "강원", # 강원
        "제천시": "충북", "청주시": "충북", "충주시": "충북", # 충북
        "계룡시": "충남", "공주시": "충남", "논산시": "충남", "당진시": "충남", "보령시": "충남", "서산시": "충남", "아산시": "충남", "천안시": "충남", # 충남
        "군산시": "전북", "김제시": "전북", "남원시": "전북", "익산시": "전북", "전주시": "전북", "정읍시": "전북",
        "광양시": "전남", "나주시": "전남", "목포시": "전남", "순천시": "전남", "여수시": "전남", # 전남
        "포항시": "경북", "경주시": "경북", "김천시": "경북", "안동시": "경북", "구미시": "경북", # 경북
        "영주시": "경북", "영천시": "경북", "상주시": "경북", "문경시": "경북", "경산시": "경북",
        "창원시": "경남", "진주시": "경남", "통영시": "경남", "사천시": "경남", "김해시": "경남", "밀양시": "경남", "거제시": "경남", "양산시": "경남", # 경남
        "제주시": "제주", "서귀포시": "제주", # 제주
    }
    metropolitan_city = ["서울", "광주", "인천", "부산", "세종", "울산", "대구", "대전"] # 특별시, 광역시
    iso_code_dict = {
        "부산": "KR-26",
        "충북": "KR-43",
        "충남": "KR-44",
        "대구": "KR-27",
        "대전": "KR-30",
        "강원": "KR-42",
        "광주": "KR-29",
        "경기": "KR-41",
        "경북": "KR-47",
        "경남": "KR-48",
        "인천": "KR-28",
        "제주": "KR-49",
        "전북": "KR-45",
        "전남": "KR-46",
        "세종": "KR-50",
        "서울": "KR-11",
        "울산": "KR-31",
    }
    for data in obj:
        occured_date = data["PRD_DE"]
        sigungu = data["C2_NM"] # 도시명
        category_code = data["C1"]
        if category_code == "01" or category_code == "03" or sigungu == '기타도시' or sigungu == '도시이외':
            continue
        category = category_dict[category_code[0:2]]
        subcategory = data["C1_NM"]
        crime_count = data["DT"]
        # crime_count가 '-' 인 경우
        if crime_count == '-' or crime_count == None:
            crime_count = 0
        # 마산이나 진해는 창원으로 통합.
        if sigungu == '마산시' or sigungu == '진해시':
            sigungu = '창원시'
        # 시도 / 시군구 분류.
        if sigungu in metropolitan_city:
            sido = sigungu
            sigungu = None
        else:
            sido = do_dict[sigungu]
        iso_code = iso_code_dict[sido]
        
        records.append(
            [
                int(occured_date),
                sido,
                sigungu,
                int(crime_count),
                category,
                subcategory,
                iso_code
            ]
        ) # 범죄발생년도, 시도, 시군구, 발생건수, 범죄대분류, 범죄중분류
        
    logging.info("Transform ended")
    return records


@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    """
    records = [
        [ 발생년도, 시도, 시군구, 발생 건 수, 범죄대분류, 범죄중분류 ],
        [ 2022-01-01, 광역자치단체, 서울특별시, 35, 강력범죄, 살인기수],
        [ 2022-01-01, 광역자치단체, 부산광역시, 68, 강력범죄, 살인미수], 
        [ 2022-01-01, 경기, 고양시, 68, 강력범죄, 살인미수], 
        ...
    ]
    """
    
    cur.execute(f"""DROP TABLE IF EXISTS {schema}.{table};""")
    create_table_sql = f"""CREATE TABLE {schema}.{table} (
        occured_date int,
        sido varchar(30),
        sigungu varchar(30),
        crime_count int,
        category varchar(30),
        subcategory varchar(30),
        iso_code varchar(30)
    );"""
    
    cur.execute(create_table_sql)
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};") 
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            occured_date = r[0]
            sido = r[1]
            sigungu = r[2]
            crime_count = r[3]
            category = r[4]
            subcategory = r[5]
            iso_code = r[6]
            if sigungu == None:
                sql = f"INSERT INTO {schema}.{table} VALUES ('{occured_date}', '{sido}', NULL, '{crime_count}', '{category}', '{subcategory}', '{iso_code}')"
            else:
                sql = f"INSERT INTO {schema}.{table} VALUES ('{occured_date}', '{sido}', '{sigungu}', '{crime_count}', '{category}', '{subcategory}', '{iso_code}')"
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;") 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
    logging.info("load done")


with DAG(
    dag_id='crime_region',
    start_date=datetime(2024, 6, 10), # 날짜가 미래인 경우 실행이 안됨
    schedule='@yearly', # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:


    url = Variable.get("api_url")
    schema = 'hhee2864'   ## 자신의 스키마로 변경
    table = 'crime_region'
    
    s3 = api_get_upload_s3(url)
    obj = extract()
    records = transform(obj)
    load(schema, table, records)
