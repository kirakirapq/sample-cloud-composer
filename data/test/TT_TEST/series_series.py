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

date = datetime.today().strftime("%Y-%m-%d")

dag = DAG(f"TT-TEST_series_series_{date}", default_args=original_args, schedule_interval=schedule_interval)


### 並列処理タスクテスト START
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

task01 = get_big_query_task_model(
  task_id = "TEST-Parallel-Task1",
  dag = dag,
  sql = sql1,
  replace_field_name = "target_date",
  gcp_conn_id = "gree-anger-dev-bigquery",
  query_params = query_params_01,
  destination_dataset_table = destination_dataset_table)

task02 = get_big_query_task_model(
  task_id = "TEST-Parallel-Task2",
  dag = dag,
  sql = sql2,
  replace_field_name = "dt",
  gcp_conn_id = "gree-anger-dev-bigquery",
  write_disposition = "WRITE_APPEND",
  query_params = query_params_02,
  destination_dataset_table = destination_dataset_table)

task_list1 = [task01, task02]

from_date = datetime.strptime("2020-10-01", '%Y-%m-%d')
to_date = datetime.strptime("2020-12-30", '%Y-%m-%d')
######### 並列処理タスクテスト END


### 直列処理タスクテスト START
interval_2 = create_interval("weekly", 1)
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

task_2_01 = get_big_query_task_model(
  task_id = "TEST-Series-Task1",
  dag = dag,
  sql = sql1,
  replace_field_name = "target_date",
  gcp_conn_id = "gree-anger-dev-bigquery",
  query_params = query_params_01,
  destination_dataset_table = destination_dataset_table)

task_2_02 = get_big_query_task_model(
  task_id = "TEST-Series-Task2",
  dag = dag,
  sql = sql2,
  replace_field_name = "dt",
  gcp_conn_id = "gree-anger-dev-bigquery",
  write_disposition = "WRITE_APPEND",
  query_params = query_params_02,
  destination_dataset_table = destination_dataset_table)

task_list_2 = [task_2_01, task_2_02]

from_date_2 = datetime.strptime("2020-10-01", '%Y-%m-%d')
to_date_2 = datetime.strptime("2020-12-30", '%Y-%m-%d')
######### 直列execute_series_task処理タスクテスト END



execute_task = ExecuteTaskList(dag)
execute_task.make_task_list(task_list1, from_date, to_date, interval).execute_series_task().make_task_list(task_list_2, from_date_2, to_date_2, interval_2).execute_series_task()
