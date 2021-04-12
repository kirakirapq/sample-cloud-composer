from datetime import datetime, timedelta
import os
import sys
import traceback

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
sys.path.append(Variable.get('module_path'))
from functions import create_interval, get_sql, get_big_query_task_model, ExecuteTaskList

# interval: daily 5
# start: 2021-01-01
# end: 2021-01-31
# destination_dataset_table: your_project_id.dataset.table$YYYYMMDD

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

dag = DAG("TEST_PRJ-T002", default_args=original_args, schedule_interval=schedule_interval)

interval = create_interval("daily", 5)

destination_dataset_table = "destination_dataset_table: your_project_id.dataset.table"
sql1 = get_sql("TEST_PRJ/test2.sql")
query_params_01 = [
    {
    "name": "dt",
    "parameterType": {
      "type": "DATE"
    },
    "parameterValue": {
      "value": "{{ params.target_date }}"
    }
  }
]

task01 = get_big_query_task_model(
  task_id = "task01",
  dag = dag,
  sql = sql1,
  gcp_conn_id = "your-conne-id",
  query_params = query_params_01,
  is_partition = True,
  destination_dataset_table = destination_dataset_table)


tasks = [task01]

from_date = datetime.strptime("2021-01-01", '%Y-%m-%d')
to_date = datetime.strptime("2021-01-31", '%Y-%m-%d')



execute_task = ExecuteTaskList(dag, tasks, from_date, to_date, interval)
execute_task.make_task_list()
execute_task.execute_series_task()
