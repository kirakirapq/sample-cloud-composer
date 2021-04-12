import json
import os
import sys
from airflow.models import Variable
sys.path.append(Variable.get('module_path'))
# sys.path.append(os.getenv('MODULE_PATH'))
from models.dag_task_model import DagTaskModel
from models.notification_message_model import NotificationMessageModel, NotificationType
from models.notification_subject_model import NotificationSubjectModel
from models.slack_model import SlackModel
from messengers.slack_notification import SlackNotification
# import Log


class SlackNotificationService:
  def __init__(self, notification_type: str) -> None:
      self.notification_type = NotificationType(notification_type)
      self.COUNT = 0
      self.RETRY_LIMIT = 5

  def send_message(self, status: str) -> dict:
      dag_id = str(status['dag']).translate({'<:': '', '>': ''})
      task_id = str(status['task']).translate({'<': '', '>': ''})

      dag_task = DagTaskModel(dag_id, task_id)

      notification_message: NotificationMessageModel = NotificationMessageModel(dag_task, self.notification_type)
      notification_subject: NotificationSubjectModel = NotificationSubjectModel(dag_task)
      slack_model = SlackModel(notification_subject, notification_message)
      self.slack: SlackNotification = SlackNotification(slack_model)

      result = None
      success = False
      while success is False:
        result: dict = self.slack.send_message()
        success = result['is_success']
        self.COUNT += 1

        if self.COUNT >= self.RETRY_LIMIT:
          print(f"SlackNotificationService.send_message() error: {result['description']}")
          # Log.error('SlackNotificationService.send_message() error', result['description'])

          raise Exception(result['description'])

      return result
