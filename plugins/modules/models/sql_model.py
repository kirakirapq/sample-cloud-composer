import os
from airflow.models import Variable

class SqlModel:
    # SQL_DIR = os.getenv('SQL_PATH')
    SQL_DIR = Variable.get("sql_path")

    def create_sql(self, sql: str, params: dict) -> str:
        """[summary]

        Args:
            sql (str): [description]
            params (dict): [description]

        Returns:
            str: [description]
        """
        def parse_sql(sql: str, params: dict) -> str:
            """[summary]

            Args:
                sql (str): [description]
                params (dict): [description]

            Returns:
                str: [description]
            """
            file_name = f'{self.SQL_DIR}/{sql}'

            with open(file_name) as f:
                txt = f.read()

            return txt.format(params)

        return parse_sql(sql, params)
