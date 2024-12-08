import json
import boto3
import os

step_functions_client = boto3.client('stepfunctions')
sns_client = boto3.client('sns')
STEP_FUNCTION_ARN = os.getenv('STEP_FUNCTION_ARN')
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')

print("Triggering Lambda Function")

def lambda_handler(event, context):
    try:
        response = step_functions_client.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN
        )

        execution_arn = response['executionArn']
        send_sns_notification(execution_arn)
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Step Function triggered successfully!',
                'executionArn': execution_arn
            })
        }
    except Exception as e:
        send_sns_notification(error=e)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to trigger Step Function.',
                'error': str(e)
            })
        }

def send_sns_notification(execution_arn=None, error=None):
    try:
        subject = 'Details about Step Function Execution'
        if execution_arn:
            message = (
                f"The Step Function has been triggered successfully.\n\n"
                f"Execution ARN: {execution_arn}"
            )
            response = sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=subject,
                Message=message
            )
        elif error:
            message = (
                f"The Step Function could not be triggered.\n\n"
                f"Error: {execution_arn}"
            )
            response = sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=subject,
                Message=message
            )
    except Exception as e:
        print(f"Failed to send SNS notification: {str(e)}")