from datetime import datetime, timedelta
import os
import sys
import traceback

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
sys.path.append(Variable.get('module_path'))
from functions import create_interval, get_sql, get_big_query_task_model, ExecuteTaskList


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

dag = DAG("TT-TEST_QP05_2021-04-01", default_args=original_args, schedule_interval=schedule_interval)

interval = create_interval("daily", 1)

destination_dataset_table = "gree-ua-kpi-dev:sample_takeuchi_dataset_tokyo.q_dau"
sql1 = get_sql("TT_TEST/qp03_01.sql")
query_params_01 = [
    {
    "name": "target_date",
    "parameterType": {
      "type": "DATE"
    },
    "parameterValue": {
      "value": "2020-10-01"
    }
  }
]

sql2 = get_sql("TT_TEST/qp03_02.sql")
query_params_02 = [
    {
    "name": "dt",
    "parameterType": {
      "type": "DATE"
    },
    "parameterValue": {
      "value": "2020-10-30"
    }
  }
]

dumy01 = DummyOperator(task_id='dummy01', dag = dag)
dumy02 = DummyOperator(task_id='dummy02', dag = dag)

task01 = get_big_query_task_model(
  task_id = "qp03_01",
  dag = dag,
  sql = sql1,
  replace_field_name = "target_date",
  gcp_conn_id = "gree-anger-dev-bigquery",
  query_params = query_params_01,
  destination_dataset_table = destination_dataset_table)

task02 = get_big_query_task_model(
  task_id = "qp03_02",
  dag = dag,
  sql = sql2,
  replace_field_name = "dt",
  gcp_conn_id = "gree-anger-dev-bigquery",
  write_disposition = "WRITE_APPEND",
  query_params = query_params_02,
  destination_dataset_table = destination_dataset_table)


tasks = [task01, task02]

from_date = datetime.strptime("2020-10-01", '%Y-%m-%d')
to_date = datetime.strptime("2020-12-30", '%Y-%m-%d')



execute_task = ExecuteTaskList(dag, tasks, from_date, to_date, interval)
execute_task.make_task_list()
execute_task.execute_parallel_task()
