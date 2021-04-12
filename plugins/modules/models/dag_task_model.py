from datetime import datetime

class DagTaskModel:
    def __init__(self, dag_id: str, task_id: str) -> None:
        dt = datetime.today()

        self.dag_id = dag_id
        self.task_id = task_id
        self.datetime = dt.strftime('%Y/%m/%d %H:%M:%S')

    def get_dag_id(self) -> str:
        return  self.dag_id

    def get_task_id(self) -> str:
        return  self.task_id

    def get_datetime(self) -> str:
        return  self.datetime

    def get_dict(self) -> dict:
        return {
            "Dag": self.dag_id,
            "Task": self.task_id,
            "datetime": self.datetime
        }
