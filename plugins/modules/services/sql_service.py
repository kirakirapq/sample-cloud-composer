import os
import sys
from airflow.models import Variable
sys.path.append(Variable.get('module_path'))
# sys.path.append(os.getenv('MODULE_PATH'))

from models.sql_model import SqlModel

class SqlService:
    def __init__(self) -> None:
        self.model = SqlModel()

    def get_sql(self, sql: str, params: dict) -> str:
        return  self.model.create_sql(sql, params)
