from airflow import DAG

class BigQueryTask:
    def __init__(
        self,
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
        is_partition: bool = True,
        time_partitioning_type: str = "DAY") -> None:
        """[summary]

        Args:
            dag (DAG): DAG
            task_id (str): タスクID
            sql (str): SQL
            destination_dataset_table (str): 保存先テーブル
            replace_field_name (str, optional):置換するフィールド名
            write_disposition (str, optional): [description]. Defaults to "WRITE_TRUNCATE".
            location (str, optional): [description]. Defaults to "asia-northeast1".
            gcp_conn_id (str, optional): [description]. Defaults to 'google_cloud_default'.
            use_legacy_sql (bool, optional): [description]. Defaults to False.
            allow_large_results (bool, optional): [description]. Defaults to False.
            is_notification_failure (bool, optional): 通知する=True, 通知しない=False.
            is_notification_success (bool, optional): 通知する=True, 通知しない=False.
            query_params (list, optional): クエリパラメータ
        """
        self.dag = dag
        self.original_task_id = task_id
        self.task_id = task_id
        self.sql = sql
        self.destination_dataset_table = destination_dataset_table
        self.replace_field_name = replace_field_name
        self.write_disposition = write_disposition
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.allow_large_results = allow_large_results
        self.is_notification_failure = is_notification_failure
        self.is_notification_success = is_notification_success
        self.query_params = query_params
        self.is_partition = is_partition
        self.time_partitioning_type = time_partitioning_type
