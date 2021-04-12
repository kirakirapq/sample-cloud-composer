import os
import sys

from airflow.models import Variable
sys.path.append(Variable.get('module_path'))
# sys.path.append(os.getenv('MODULE_PATH'))
from models.notification_message_model import NotificationMessageModel
from models.notification_subject_model import NotificationSubjectModel

class SlackModel:
  def __init__(self, subject: NotificationSubjectModel, message: NotificationMessageModel) -> None:
    self.url = Variable.get("slack_url")
    self.channel = Variable.get("slack_channel")
    self.subject = subject
    self.message = message

  def get_url(self) -> str:
    return self.url

  def get_channel(self) -> str:
    return self.channel

  def get_subject(self) -> str:
    return self.subject.get_subject()

  def get_message(self) -> str:
    return self.message.get_message()
