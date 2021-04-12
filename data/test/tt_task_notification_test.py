from datetime import datetime, timedelta
import os
import sys
import traceback


# from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
# sys.path.append(os.path.join('/home/airflow/gcs/plugins/modules/'))
from airflow.models import Variable
sys.path.append(Variable.get('module_path'))
# sys.path.append(os.getenv('MODULE_PATH'))
from functions import get_sql, get_dag, get_bq_operator, slack_failure_notification, slack_success_notification

today       = datetime.today()
yesterday   = today - timedelta(days=1)
target_date = yesterday.strftime('%Y-%m-%d')

params =  {"table": "`gree-anger-dev.dummy_anger.access`", "target_date": "2020-12-07", "page_group": "home"}
sql = get_sql("test/test.sql", params)

# bq_service = BqOpServiceClass("DAG-test")

dag = get_dag("DAG-TEST-001")

task_1 = get_bq_operator(
    dag,
    task_id = "test_task_1",
    sql = sql,
    gcp_conn_id = "gree-anger-dev-bigquery",
    on_failure_callback=slack_failure_notification,
    on_success_callback=slack_success_notification)

task_2 = get_bq_operator(
    dag,
    task_id = "test_task_2",
    sql = sql,
    gcp_conn_id = "gree-anger-dev-bigquery",
    on_failure_callback=slack_failure_notification,
    on_success_callback=slack_success_notification)

task_1 >> task_2
