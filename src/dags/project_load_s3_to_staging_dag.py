from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.models.variable import Variable
import pendulum
import boto3
import pandas as pd
import vertica_python
import json


conn_info =  json.loads(Variable.get("VERTICA_CONN_INFO"))


def fetch_s3_file(bucket: str, key: str):

    AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

    session = boto3.session.Session()

    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    ) 
  
def transform_csv_file():
    df_group_log = pd.read_csv('/data/group_log.csv')
    df_group_log['user_id_from'] = pd.array(df_group_log['user_id_from'], dtype="Int64")
    df_group_log.to_csv('/data/group_log_ed.csv', index = False, header = False)

def load_to_vertica_staging(conn_params=conn_info):
    with vertica_python.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            with open("/data/group_log_ed.csv", "rb") as fs:
                cur.copy(
                            """
                                copy STV2023100611__STAGING.group_log (
                                    group_id,user_id,user_id_from,event,datetime)  
                                     FROM STDIN 
                                    DELIMITER ','
                                    REJECTED DATA AS TABLE group_log_rej;
                            """, fs
                        )

get_data_from_s3 = DAG(
    'project6_load_to_staging_dag',
    start_date=pendulum.parse('2022-07-13'),
    schedule_interval=None
)

fetch_group_log = PythonOperator(
    task_id='fetch_group_log',
    python_callable=fetch_s3_file,
    op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'},
    dag = get_data_from_s3
)

transform_group_log = PythonOperator(
    task_id='transform_group_log',
    python_callable=transform_csv_file,
    dag = get_data_from_s3
)

load_to_staging = PythonOperator(
    task_id='load_to_staging',
    python_callable=load_to_vertica_staging,
    dag = get_data_from_s3
)
    

fetch_group_log >> transform_group_log >> load_to_staging