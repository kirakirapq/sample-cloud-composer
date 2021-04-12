import json
import os
import requests
import sys
from airflow.models import Variable
sys.path.append(Variable.get('module_path'))
# sys.path.append(os.getenv('MODULE_PATH'))
from models.slack_model import SlackModel

class SlackNotification:
  def __init__(self, slack_model: SlackModel) -> None:
    self.slack_model = slack_model

  def get_payload(self, channel: str, text: str, subject: str) -> dict:

      return {
          'channel': f"#{channel}",
          'blocks': [
            {
            "type": "header",
              "text": {
                "type": "plain_text",
                "text": subject
            }
            },
            {
              "type": "section",
              "fields": [
                  {
                    "type": "mrkdwn",
                    "text": text
                  }
              ]
            }
          ]
      }

  def send_message(self, channel: str = None, subject: str = None, message: str = None) -> dict:
    if channel is None:
      channel = self.slack_model.get_channel()

    if subject is None:
      subject = self.slack_model.get_subject()

    if message is None:
      message = self.slack_model.get_message()

    payload   = self.get_payload(channel, message, subject)

    response = requests.post(
      self.slack_model.get_url(),
      data=json.dumps(payload),
      headers = {'Content-Type': 'application/json'})

    if response.status_code in [200, 201, 204]:
      print("---SlackNotification.send_message---")
      print(response.status_code)
      print(response.text)
      return {'is_success': True, 'description': response}

    print("---SlackNotification.send_message---")
    print(response.status_code)
    print(f"message: {response.text}, chanel: {channel}")
    return {'is_success': False, 'description': response}
