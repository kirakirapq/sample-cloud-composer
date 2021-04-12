from airflow import DAG
# https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_modules/airflow/providers/google/cloud/operators/bigquery.html#BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

def get_bq_operator_as_insert_data_with_non_query_params(
    dag: DAG,
    task_id: str,
    sql: str,
    destination_dataset_table: str,
    write_disposition: str = "WRITE_TRUNCATE",
    location: str = "asia-northeast1",
    gcp_conn_id: str = 'google_cloud_default',
    use_legacy_sql: bool = False,
    allow_large_results: bool = False,
    params: dict = None,
    on_failure_callback = None,
    on_success_callback = None) -> BigQueryExecuteQueryOperator:
  """Query_Parameterなしでインサート用のBigQueryExecuteQueryOperatorをコール

  Args:
      dag (str): [description]
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

  Returns:
      BigQueryExecuteQueryOperator: [description]
  """
  return BigQueryExecuteQueryOperator(
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


def get_bq_operator_as_insert_data_with_query_params(
    dag: DAG,
    task_id: str,
    sql: str,
    destination_dataset_table: str,
    write_disposition: str = "WRITE_TRUNCATE",
    location: str = "asia-northeast1",
    gcp_conn_id: str = 'google_cloud_default',
    use_legacy_sql: bool = False,
    allow_large_results: bool = False,
    params: dict = None,
    on_failure_callback = None,
    on_success_callback = None,
    query_params: list = None) -> BigQueryExecuteQueryOperator:
    """Query_Parameter付きでインサート用のBigQueryExecuteQueryOperatorをコール

    Args:
        dag (str): [description]
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

    return BigQueryExecuteQueryOperator(
        task_id = task_id,
        gcp_conn_id = gcp_conn_id,
        use_legacy_sql=use_legacy_sql,
        allow_large_results = allow_large_results,
        write_disposition = write_disposition,
        sql = sql,
        query_params = query_params,
        destination_dataset_table = destination_dataset_table,
        dag=dag,
        params = params,
        location= location,
        on_failure_callback = on_failure_callback,
        on_success_callback = on_success_callback)


def get_bq_operator_with_non_query_params(
    dag: DAG,
    task_id: str,
    sql: str,
    location: str = "asia-northeast1",
    gcp_conn_id: str = 'google_cloud_default',
    use_legacy_sql: bool = False,
    allow_large_results: bool = False,
    on_failure_callback = None,
    on_success_callback = None) -> BigQueryExecuteQueryOperator:
  """Query_ParameterなしSQL実行用のBigQueryExecuteQueryOperatorをコール

  Args:
      task_id ([type], optional): [description]. Defaults to task_id.
      gcp_conn_id ([type], optional): [description]. Defaults to gcp_conn_id.
      use_legacy_sql ([type], optional): [description]. Defaults to use_legacy_sql.
      allow_large_results ([type], optional): [description]. Defaults to allow_large_results.
      sql ([type], optional): [description]. Defaults to sql.
      dag ([type], optional): [description]. Defaults to dag.
      location ([type], optional): [description]. Defaults to location.
      on_failure_callback ([type], optional): [description]. Defaults to on_failure_callback.
      is_notification_success ([type], optional): [description]. Defaults to is_notification_success.

  Returns:
      BigQueryExecuteQueryOperator: [description]
  """
  return BigQueryExecuteQueryOperator(
      task_id = task_id,
      gcp_conn_id = gcp_conn_id,
      use_legacy_sql=use_legacy_sql,
      allow_large_results = allow_large_results,
      sql = sql,
      dag=dag,
      location= location,
      on_failure_callback = on_failure_callback,
      on_success_callback = on_success_callback)

def get_bq_operator_with_query_params(
    dag: DAG,
    task_id: str,
    sql: str,
    location: str = "asia-northeast1",
    gcp_conn_id: str = 'google_cloud_default',
    use_legacy_sql: bool = False,
    allow_large_results: bool = False,
    on_failure_callback = None,
    on_success_callback = None,
    query_params: list = None) -> BigQueryExecuteQueryOperator:
  """Query_Parameter付きでSQL実行用のBigQueryExecuteQueryOperatorをコール

  Args:
      task_id ([type], optional): [description]. Defaults to task_id.
      gcp_conn_id ([type], optional): [description]. Defaults to gcp_conn_id.
      use_legacy_sql ([type], optional): [description]. Defaults to use_legacy_sql.
      allow_large_results ([type], optional): [description]. Defaults to allow_large_results.
      sql ([type], optional): [description]. Defaults to sql.
      query_params ([type], optional): [description]. Defaults to query_params.
      dag ([type], optional): [description]. Defaults to dag.
      location ([type], optional): [description]. Defaults to location.
      on_failure_callback ([type], optional): [description]. Defaults to on_failure_callback.
      is_notification_success ([type], optional): [description]. Defaults to is_notification_success.

  Returns:
      BigQueryExecuteQueryOperator: [description]
  """
  return BigQueryExecuteQueryOperator(
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
