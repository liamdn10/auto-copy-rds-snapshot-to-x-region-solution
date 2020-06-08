import json
import boto3
import os
import sys


class SnsClient:
    def __init__(self):
        self.SNS_TOPIC_ARN = os.environ.get('sns_topic_arn')
        try:
            self.__sns_client = boto3.client('sns')
        except Exception as e:
            print("ERROR: failed to connect to SNS")
            error_notification(e)
            sys.exit(1)
        
    def error_notification(self, e):
        self.__sns_client.publish(
            TopicArn = self.SNS_TOPIC_ARN,
            Message = str(e),
            Subject = 'Auto Copy RDS Snapshot To X Region Notification'
        )