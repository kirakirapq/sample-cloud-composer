from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import sys
from airflow.models import Variable
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_modules/airflow/providers/google/cloud/operators/bigquery.html#BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
sys.path.append(Variable.get('module_path'))
from bq_operators import (
    get_bq_operator_as_insert_data_with_non_query_params,
    get_bq_operator_as_insert_data_with_query_params,
    get_bq_operator_with_non_query_params,
    get_bq_operator_with_query_params)
from models.big_query_task import BigQueryTask
from models.execute_interval_model import ExecuteInterval, IntervalDaily, IntervalWeekly, IntervalMonthly
from services.sql_service import SqlService
from services.slack_notification_service import SlackNotificationService

def get_sql(sql_file: str, params: dict = {}):
    service = SqlService()

    return service.get_sql(sql_file, params)

def get_dag(dag_name: str, default_args: dict = None, schedule_interval: timedelta = None) -> DAG:
    one_day_ago = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())
    original_args = {
            "owner": "airflow",
            "depends_on_past": False,
            "start_date": one_day_ago,
            "email": ["airflow@airflow.com"],
            "email_on_failure": False, # Trueにするとtaskが失敗した際にメールが通知
            "email_on_retry": False, # Trueにするとtaskがリトライが発生した際にメールが通知
            "retries": 1,
            "retry_delay": timedelta(minutes=10)}

    if default_args is not None:
        for key, value in default_args.items():
            original_args[key] = value # 更新

    if schedule_interval is None:
        schedule_interval = timedelta(days=1)
    # DAGを定義
    return DAG(dag_name, default_args=original_args, schedule_interval=schedule_interval)

def get_bq_operator_as_insert_data(
    dag: DAG,
    task_id: str,
    sql: str,
    destination_dataset_table: str,
    write_disposition: str = "WRITE_TRUNCATE",
    location: str = "asia-northeast1",
    gcp_conn_id: str = 'google_cloud_default',
    use_legacy_sql: bool = False,
    allow_large_results: bool = False,
    is_notification_failure: bool = False,
    is_notification_success: bool = False,
    query_params: list = None,
    params: dict = None) -> BigQueryExecuteQueryOperator:
    """実行したSQL結果を「destination_dataset_table」へ保存するタスク

    Args:
        dag (DAG): [description]
        task_id (str): [description]
        sql (str): [description]
        destination_dataset_table (str): [description]
        write_disposition (str, optional): [description]. Defaults to "WRITE_TRUNCATE".
        location (str, optional): [description]. Defaults to "asia-northeast1".
        gcp_conn_id (str, optional): [description]. Defaults to 'google_cloud_default'.
        use_legacy_sql (bool, optional): [description]. Defaults to False.
        allow_large_results (bool, optional): [description]. Defaults to False.
        is_notification_failure (bool, optional): [description]. Defaults to False.
        is_notification_success (bool, optional): [description]. Defaults to False.
        query_params (list, optional): [description]. Defaults to None.

    Returns:
        BigQueryExecuteQueryOperator: [description]
    """

    on_failure_callback = None
    on_success_callback = None

    if is_notification_failure is True:
        on_failure_callback = slack_failure_notification

    if is_notification_success is True:
        on_success_callback = slack_success_notification

    if query_params is None:
        return get_bq_operator_as_insert_data_with_non_query_params(
            task_id = task_id,
            gcp_conn_id = gcp_conn_id,
            use_legacy_sql=use_legacy_sql,
            allow_large_results = allow_large_results,
            write_disposition = write_disposition,
            sql = sql,
            destination_dataset_table = destination_dataset_table,
            dag=dag,
            params = params,
            location= location,
            on_failure_callback = on_failure_callback,
            on_success_callback = on_success_callback)

    return get_bq_operator_as_insert_data_with_query_params(
            task_id = task_id,
            gcp_conn_id = gcp_conn_id,
            use_legacy_sql=use_legacy_sql,
            allow_large_results = allow_large_results,
            write_disposition = write_disposition,
            sql = sql,
            query_params = query_params,
            params = params,
            destination_dataset_table = destination_dataset_table,
            dag=dag,
            location= location,
            on_failure_callback = on_failure_callback,
            on_success_callback = on_success_callback)


def get_bq_operator_as_get_data(
    dag: DAG,
    task_id: str,
    sql: str,
    location: str = "asia-northeast1",
    gcp_conn_id: str = 'google_cloud_default',
    use_legacy_sql: bool = False,
    allow_large_results: bool = False,
    is_notification_failure: bool = False,
    is_notification_success: bool = False,
    query_params: list = None) -> BigQueryExecuteQueryOperator:
    """実行したSQL結果を取得するタスク

    Args:
        dag (DAG): [description]
        task_id (str): [description]
        sql (str): [description]
        location (str, optional): [description]. Defaults to "asia-northeast1".
        gcp_conn_id (str, optional): [description]. Defaults to 'google_cloud_default'.
        use_legacy_sql (bool, optional): [description]. Defaults to False.
        allow_large_results (bool, optional): [description]. Defaults to False.
        is_notification_failure (bool, optional): [description]. Defaults to False.
        is_notification_success (bool, optional): [description]. Defaults to False.
        query_params (list, optional): [description]. Defaults to None.

    Returns:
        BigQueryExecuteQueryOperator: [description]
    """
    on_failure_callback = None
    on_success_callback = None

    if is_notification_failure is True:
        on_failure_callback = slack_failure_notification

    if is_notification_success is True:
        on_success_callback = slack_success_notification

    if query_params is None:
        return get_bq_operator_with_non_query_params(
            task_id = task_id,
            gcp_conn_id = gcp_conn_id,
            use_legacy_sql=use_legacy_sql,
            allow_large_results = allow_large_results,
            sql = sql,
            dag=dag,
            location= location,
            on_failure_callback = on_failure_callback,
            on_success_callback = on_success_callback)

    return get_bq_operator_with_query_params(
            task_id = task_id,
            gcp_conn_id = gcp_conn_id,
            use_legacy_sql=use_legacy_sql,
            allow_large_results = allow_large_results,
            sql = sql,
            query_params = query_params,
            dag=dag,
            location= location,
            on_failure_callback = on_failure_callback,
            on_success_callback = on_success_callback)


def slack_failure_notification(status: str) -> None:
    failure = SlackNotificationService("failure")
    failure.send_message(status)



def slack_success_notification(status: str) -> None:
    success = SlackNotificationService("success")
    success.send_message(status)


def get_big_query_task_model(
    dag: DAG,
    task_id: str,
    sql: str,
    destination_dataset_table: str,
    replace_field_name: str = "target_date",
    write_disposition: str = "WRITE_TRUNCATE",
    location: str = "asia-northeast1",
    gcp_conn_id: str = 'google_cloud_default',
    use_legacy_sql: bool = False,
    allow_large_results: bool = False,
    is_notification_failure: bool = False,
    is_notification_success: bool = False,
    query_params: list = None,
    is_partition: bool = False,
    time_partitioning_type: str = "DAY") -> BigQueryTask:

    return BigQueryTask(
        dag,
        task_id,
        sql,
        destination_dataset_table,
        replace_field_name,
        write_disposition,
        location,
        gcp_conn_id,
        use_legacy_sql,
        allow_large_results,
        is_notification_failure,
        is_notification_success,
        query_params,
        is_partition,
        time_partitioning_type)

def create_interval(type: str, value: int) -> ExecuteInterval:
    """ExecuteIntervalを生成する

    Args:
        type (str): daily or weekly or monthly
        value (int): 整数値を指定

    Returns:
        ExecuteInterval: [description]
    """
    if type.lower() == "daily":
        return ExecuteInterval(IntervalDaily(value))

    if type.lower() == "weekly":
        return ExecuteInterval(IntervalWeekly(value))

    return ExecuteInterval(IntervalMonthly(value))


class ExecuteTaskList:
    def __init__(
        self,
        dag: DAG,
        task_list: list = None,
        from_date: datetime = None,
        to_date: datetime = None,
        execute_interval: ExecuteInterval = None) -> None:
        self.dag = dag
        self.task_list = task_list
        self.from_date = from_date
        self.to_date = to_date
        self.execute_interval = execute_interval
        self._call_cnt = 1
        self.start_task = self.get_dummy_task("start")

    def get_dummy_task(self, task_name: str) -> DummyOperator:
        task_id = f'{task_name}_{self._call_cnt}'
        return DummyOperator(task_id=task_id, dag = self.dag)


    def make_task_list(
        self,
        task_list: list = None,
        from_date: datetime = None,
        to_date: datetime = None,
        execute_interval: ExecuteInterval = None):
        """from_date からto_dateまでexecute_intervalの間隔でタスクをリスト順に実行する

        Args:
            tasks (list): [BigQueryTask]
            from_date (datetime): [description]
            to_date (datetime): [description]
            execute_interval (ExecuteInterval): [description]
        """
        self._call_cnt += 1
        dag = self.dag

        if task_list is None:
            task_list = self.task_list

        if from_date is None:
            from_date = self.from_date

        if to_date is None:
            to_date = self.to_date

        if execute_interval is None:
            execute_interval = self.execute_interval

        if task_list is None or from_date is None or to_date is None or execute_interval is None:
            print(f"Error class ExecuteTaskList.make_task_list: tasks or from_date or to_date or execute_interval is not set.")
            sys.exit()

        interval = execute_interval.get_interval()
        interval_type = interval["Type"]
        interval_value = interval["Interval"]
        print(f"ExecuteTaskList.make_task_list: interval_type: {interval_type}, interval_value: {interval_value}")

        def get_task_id(task:BigQueryTask, suffix: str) -> None:
            """task_idを task_id_YYYYMMDDへ変換する
            Args:
                task (BigQueryTask): [description]
                suffix (str): [description]
            """
            # task.task_id = f'{task.original_task_id}_{suffix}'
            return f'{task.original_task_id}_{suffix}'

        def get_table_with_partition(task: BigQueryTask, current_date: datetime) -> str:
            """保存先テーブルがパーテーションの場合はパーテションをセット

            Args:
                task (BigQueryTask): [description]
                current_date (datetime): [description]
            """
            if task.is_partition is True:
                if task.time_partitioning_type.upper() == "DAY":
                    target_date = current_date.strftime('%Y%m%d')

                    return f'{task.destination_dataset_table}${target_date}'

            return task.destination_dataset_table

        print(f"ExecuteTaskList.make_task_list: from_date: {from_date}, to_date: {to_date}, interval_type: {interval_type}, interval_value: {interval_value}")

        current_date = from_date
        # 日付ごとにchildが入る
        parents = []
        parent_test = []
        while current_date <= to_date:
            target_date = current_date.strftime('%Y-%m-%d')
            partition_date = current_date.strftime('%Y%m%d')
            print("ExecuteTaskList.make_task_list", f'current_date: {target_date}')

             # ある日に実行されるタスク
            childlen = []
            childlen_test = []
            for n, task in enumerate(task_list):
                if not isinstance(task, BigQueryTask):
                    continue

                task_id = get_task_id(task, target_date)
                destination_dataset_table = get_table_with_partition(task, current_date)

                op = None
                # operater(Task)を生成
                op = get_bq_operator_as_insert_data(
                    dag = dag,
                    task_id = task_id,
                    sql = task.sql,
                    destination_dataset_table = destination_dataset_table,
                    write_disposition = task.write_disposition,
                    location = task.location,
                    gcp_conn_id = task.gcp_conn_id,
                    use_legacy_sql = task.use_legacy_sql,
                    allow_large_results = task.allow_large_results,
                    is_notification_failure = task.is_notification_failure,
                    is_notification_success = task.is_notification_success,
                    params = dict(target_date = target_date, partition_date = partition_date),
                    query_params = task.query_params)
                print("------ タスクインスタンス生成後---------")
                print(f'get_bq_operator_as_insert_data >> query_params: {op.query_params}')
                print("---------------")

                childlen.append(op)
                childlen_test.append(task.task_id)

            if interval_type == "daily":
                current_date += timedelta(days=interval_value)
            if interval_type == "weekly":
                current_date += relativedelta(weeks=interval_value)
            if interval_type == "monthly":
                current_date += relativedelta(months=interval_value)

            parents.append(childlen)
            parent_test.append(childlen_test)

        self.parents = parents
        self.parent_test = parent_test

        return self

    def execute_parallel_task(self):
        start = self.start_task
        end = self.get_dummy_task('end')
        index = 1
        upstream_seams = self.get_dummy_task(f'seams_0{index}')

        op_list = self.parents
        if type(op_list) is list:
            for i, childlen in enumerate(op_list):
                print(f"dummy_task_id: seams_0{i}")
                if i > 0:
                    upstream_seams = downstream_seams

                if i == len(op_list):
                    downstream_seams = end
                else:
                    downstream_seams = self.get_dummy_task(f'seams_0{index + i + 1}')

                if i == 0:
                    start >> childlen >> downstream_seams
                else:
                    upstream_seams >> childlen >> downstream_seams

        self.start_task = end

        return self

    def execute_series_task(self):
        import itertools
        op_list = list(itertools.chain.from_iterable(self.parents))

        start = self.start_task
        end = self.get_dummy_task('end')

        for i, op in enumerate(op_list):
            if i == 0:
                start >> op
            else:
                op_list[i-1] >> op

        op_list[len(op_list) - 1] >> end

        self.start_task = end

        return self
