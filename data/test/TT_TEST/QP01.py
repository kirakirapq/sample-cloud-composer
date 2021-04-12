from datetime import datetime, timedelta
import os
import sys
import traceback

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
sys.path.append(Variable.get('module_path'))
from functions import get_sql, get_bq_operator_as_get_data


one_day_ago = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())
original_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": one_day_ago,
        "email": ["airflow@airflow.com"],
        "email_on_failure": False, # Trueにするとtaskが失敗した際にメールが通知
        "email_on_retry": False, # Trueにするとtaskがリトライが発生した際にメールが通知
        "retries": 1,
        "retry_delay": timedelta(minutes=10)}

schedule_interval = timedelta(days=1)

dag = DAG("TT-TEST_QP01_2021-04-05", default_args=original_args, schedule_interval=schedule_interval)

sql = get_sql("TT_TEST/get_data_query_params.sql")
query_params = [
    {
    "name": "from_date",
    "parameterType": {
      "type": "DATE"
    },
    "parameterValue": {
      "value": "2020-10-01"
    }
  },
  {
    "name": "end_date",
    "parameterType": {
      "type": "DATE"
    },
    "parameterValue": {
      "value": "2020-10-01"
    }
  }
]

dumy01 = DummyOperator(task_id='dummy01', dag = dag)
dumy02 = DummyOperator(task_id='dummy02', dag = dag)

task01 = get_bq_operator_as_get_data(
  task_id = "get01",
  dag = dag,
  sql = sql,
  gcp_conn_id = "gree-anger-dev-bigquery",
  query_params = query_params)


dumy01 >> task01 >> dumy02
