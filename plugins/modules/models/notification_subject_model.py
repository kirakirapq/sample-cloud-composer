import os
import sys

from airflow.models import Variable
sys.path.append(Variable.get('module_path'))
# sys.path.append(os.getenv('MODULE_PATH'))
from airflow.models import Variable
from models.dag_task_model import DagTaskModel

class NotificationSubjectModel:
  def __init__(self, dag_task_model: DagTaskModel, title: str = None) -> None:
    def set_subject(dag_task_model: DagTaskModel, title: str = None) -> None:
      """[summary]

      Args:
          dag_task_model (DagTaskModel): [description]
          msg_type (str): [description]
          title (str, optional): [description]. Defaults to None.

      Returns:
          [type]: [description]
      """
      if title is not None:
        return title

      subject = Variable.get("slack_subject")

      return subject.format(dag_task_model.get_dict())

    self.subject: str = set_subject(dag_task_model, title)

  def get_subject(self) -> str:
    return self.subject
