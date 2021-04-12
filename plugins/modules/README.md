## モジュール一覧

|モジュール|パラメータ|戻り値|説明|
|:--|:--|:--|:--|
|get_sql|sql_file: str, params: dict = {}|sql: str|オプションでパラメータを引数に渡すとSQLを置換|

* **例** : sql = get_sql(sql_path, {"table_name": "hoge:hoge.hoge"})


|モジュール|パラメータ|戻り値|説明|
|:--|:--|:--|:--|
|get_dag|dag_name: str, default_args: dict = None, schedule_interval: timedelta = None|DAG|DAGを生成|

* **パラメータ**
* dag_name: ダグ名
* default_args: DAGのデフォルトパラメータ
* schedule_interval: 実行間隔

* 例: dag = get_dag("sample-dag")


|モジュール|パラメータ|戻り値|説明|
|:--|:--|:--|:--|
|get_bq_operator_as_insert_data||BigQueryExecuteQueryOperator|データ保存先を指定してBigQueryExecuteQueryOperatorを生成|

* **パラメータ**
* dag: DAG,
* task_id: str,
* sql: str,
* destination_dataset_table: str,
* write_disposition: str = "WRITE_TRUNCATE",
* location: str = "asia-northeast1",
* gcp_conn_id: str = 'google_cloud_default',
* use_legacy_sql: bool = False,
* allow_large_results: bool = False,
* is_notification_failure: bool = False,
* is_notification_success: bool = False,
* query_params: list = None


|モジュール|パラメータ|戻り値|説明|
|:--|:--|:--|:--|
|get_bq_operator_as_get_data||BigQueryExecuteQueryOperator|のBigQueryExecuteQueryOperatorを生成|

* **パラメータ**
* dag: DAG,
* task_id: str,
* sql: str,
* location: str = "asia-northeast1",
* gcp_conn_id: str = 'google_cloud_default',
* use_legacy_sql: bool = False,
* allow_large_results: bool = False,
* is_notification_failure: bool = False,
* is_notification_success: bool = False,
* query_params: list = None)

|モジュール|パラメータ|戻り値|説明|
|:--|:--|:--|:--|
|slack_failure_notification|state: str|None|BaseOperatorのコールバックに指定するとタスク失敗時にslack通知を行う|

|モジュール|パラメータ|戻り値|説明|
|:--|:--|:--|:--|
|slack_success_notification|state: str|None|BaseOperatorのコールバックに指定するとタスク成功時にslack通知を行う|


|モジュール|パラメータ|戻り値|説明|
|:--|:--|:--|:--|
|get_big_query_task_model||BigQueryTask|BigQueryTaskはExecuteTaskListの引数(list型で指定)|

* **パラメータ**
* dag: DAG
* task_id: str
* sql: str
* destination_dataset_table: str
* write_disposition: str = "WRITE_TRUNCATE"
* location: str = "asia-northeast1"
* gcp_conn_id: str = 'google_cloud_default'
* use_legacy_sql: bool = False
* allow_large_results: bool = False
* is_notification_failure: bool = False
* is_notification_success: bool = False
* query_params: list = None

* query_params

```
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
```


|モジュール|パラメータ|戻り値|説明|
|:--|:--|:--|:--|
|create_interval|type: str, value: int|ExecuteInterval|ExecuteIntervalはExecuteTaskListの引数|

* **パラメータ**
* type: str (daily | weekly | monthly のいずれかを指定)
* value: int

* **例** : daily_task = create_interval("daily", 1)


|モジュール|パラメータ|戻り値|説明|
|:--|:--|:--|:--|
|ExecuteTaskList|DAG, [BigQueryTask], 開始日:datetime, 終了日:datetime, インターバル:ExecuteInterval||日付範囲を指定してDAGを生成する|


#### **メソッド:  __init(dag:DAG, task_list: list, from_date: datetime, to_date: datetime,execute_interval: ExecuteInterval)__**
* **パラメータ**
* dag: DAG (必須)
* task_list: list = None (BigQueryTaskのリスト)
* from_date: datetime = None,
* to_date: datetime = None,
* execute_interval: ExecuteInterval = None

#### **メソッド:  make_task_list(task_list: list, from_date: datetime, to_date: datetime,execute_interval: ExecuteInterval)**
#### 概要
* 指定範囲の間、インターバルごとにtask_listのタスクを生成します
* **パラメータ**
* task_list: list = None (BigQueryTaskのリスト)
* from_date: datetime = None,
* to_date: datetime = None,
* execute_interval: ExecuteInterval = None

#### **メソッド:  execute_series_task()**
#### 概要
* 生成したタスクから直列ワークフローを作成します

* **例** 

```
execute_task = ExecuteTaskList(dag, tasks, from_date, to_date, interval)
execute_task.make_task_list()
execute_task.execute_series_task()
```
