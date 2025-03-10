# Auto-Ingestion Pipeline for S3 to Snowflake

## Architecture and about pipeline
This project automates data ingestion from AWS S3 to Snowflake using Snowpipe. It allows users to upload JSON data via AWS Transfer Family, which is automatically ingested into Snowflake. AWS SNS and SQS are used for monitoring and alerting. The figure in the architecture.png is a simple representation of the architecture of this pipeline

When a user/transaction database loads a data file in the AWS S3 bucket, the snowpipe (auto ingest) gets triggered and copies this data into specified table. Once snowpipe processes a new file, it sends a notification to the SQS queue. This notification contains details about the ingested file. The SQS then triggers a lambda function which further handles the messaging via SNS.

lambda_function.py : the lambda function which matches file path to identify file type ingested
sftp_read_write_policy : policy attached to the iam role who adds or updates files to S3 bucket
snowflake_code.sql : snowflake setup for storage integration, stage object, database, schema, ...
storage_integration_policy : policy attached to IAM role which was used in storage integration