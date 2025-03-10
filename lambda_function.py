import json
import boto3

sqs_client = boto3.client('sqs')
sns_client = boto3.client('sns')

SNS_TOPIC_ARN = '<sns arn>'

def lambda_handler(event):
    for record in event['Records']:
        message_body = record['body']
        message = json.loads(message_body)  # Snowflake's message (json format)

        # checking the file path to determine the pipe
        if message.get('file_name', '')!='' and 'user_data' == message.get('file_name', '')[:9]:
            subject = 'New User Data Ingestion'
            body = f"A new file has been ingested into the user data pipe: {message.get('file_name')}"
        elif message.get('file_name', '')!='' and  'brand_data' == message.get('file_name', '')[:10]:
            subject = 'New Brand Data Ingestion'
            body = f"A new file has been ingested into the brand data pipe: {message.get('file_name')}"
        elif message.get('file_name', '')!='' and  "receipt_data" == message.get('file_name', '')[:12]:
            subject = 'New Receipt Data Ingestion'
            body = f"A new file has been ingested into the receipt data pipe: {message.get('file_name')}"
        else:
            # the message doesn't match any of the known pipes
            subject = "Unknown Data Ingestion"
            body = f"An unknown file was ingested: {message.get('file_name')}"

        # publishing to SNS
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=body,
            Subject=subject
        )

        print(f"Published message to SNS: {response}")

    return {'statusCode': 200, 'body': 'Messages processed successfully'}