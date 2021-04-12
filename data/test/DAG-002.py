from datetime import datetime, timedelta
import os
import sys
from airflow.models import Variable
sys.path.append(Variable.get('module_path'))

from functions import get_dag, get_bq_operator, get_sql

from airflow import DAG


today       = datetime.today()
yesterday   = today - timedelta(days=1)

dag = get_dag("DAG-002")

destination_dataset_table = "gree-ua-kpi-dev:sample_takeuchi_dataset_tokyo.q_dau"
params =  {"from_date": "2020-10-01", "to_date": "2020-12-30"}

sql = get_sql("test/test_a2b.sql", params)

op_1 = get_bq_operator(
    dag,
    task_id = "task1",
    sql = sql,
    gcp_conn_id = "gree-anger-dev-bigquery")

op_2 = get_bq_operator(
    dag,
    task_id = "task2",
    sql = sql,
    gcp_conn_id = "gree-anger-dev-bigquery")

op_3 = get_bq_operator(
    dag,
    task_id = "task3",
    sql = sql,
    gcp_conn_id = "gree-anger-dev-bigquery",
    destination_dataset_table = destination_dataset_table)

op_1 >> op_2
