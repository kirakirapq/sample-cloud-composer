import configparser
import os
import sys

from airflow.models import Variable
sys.path.append(Variable.get('module_path'))
# sys.path.append(os.getenv('MODULE_PATH'))
from airflow.models import Variable
from models.dag_task_model import DagTaskModel


class NotificationType:
  def __init__(self, notification_type: str) -> None:
    if notification_type is "success":
      self.notification_type = "success"
    else:
      self.notification_type = "failure"

  def get_notification_type(self) -> str:
    return self.notification_type

class NotificationMessageModel:
  def __init__(self, dag_task_model: DagTaskModel, notification_type: NotificationType) -> None:
    def create_message(dag_task_model: DagTaskModel, notification_type: NotificationType) -> None:
      """[summary]

      Args:
          dag_task_model (DagTaskModel): [description]
          notification_type (NotificationType): success | failure

      Returns:
          [type]: [description]
      """
      data = '---------------------\n'
      data = data + f'dag_id: {dag_task_model.get_dag_id()}\n'
      data = data + f'sub_code: {dag_task_model.get_task_id()}\n'
      data = data + f'datetime: {dag_task_model.get_datetime()}\n'
      data = data + '---------------------\n'

      file_name = Variable.get(f"slack_{notification_type.get_notification_type()}_text")

      with open(file_name) as f:
        txt = f.read()
        return txt.format(data = data)

    self.message: str = create_message(dag_task_model, notification_type)

  def get_message(self) -> str:
    return self.message
