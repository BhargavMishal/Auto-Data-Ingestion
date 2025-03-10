-- creating the database and schema
CREATE DATABASE IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS raw_data;
USE DATABASE raw_data;

-- creating a storage integration
CREATE STORAGE INTEGRATION IF NOT EXISTS snowflake_storage_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '<role arn>'
    STORAGE_ALLOWED_LOCATIONS = ('<s3 bucket directory>');

-- getting a couple arns to update policy
DESC INTEGRATION snowflake_storage_integration;

-- creating a stage object
CREATE OR REPLACE STAGE raw_data.aws_ext_stage_integration
    URL = 's3://<bucket name>'
    STORAGE_INTEGRATION = snowflake_storage_integration;

-- checking if the integration and stage are working
LIST @raw_data.aws_ext_stage_integration;

-- creating tables for each entitiy
CREATE OR REPLACE TABLE raw_data.users (raw_json VARIANT);
CREATE OR REPLACE TABLE raw_data.receipts (raw_json VARIANT);
CREATE OR REPLACE TABLE raw_data.brands (raw_json VARIANT);

-- creating the file format for JSON to use in pipes
CREATE OR REPLACE FILE FORMAT json_format
    TYPE = JSON;

-- creating the pipe for getting user_data
CREATE OR REPLACE PIPE aws_s3_pipe_users
    AUTO_INGEST = TRUE
    AS
    COPY INTO raw_data.users
    FROM @raw_data.aws_ext_stage_integration
    PATTERN = '.*user_data/.*\.json'
    FILE_FORMAT = (FORMAT_NAME = json_format);

-- creating the pipe for getting brand_data
CREATE OR REPLACE PIPE aws_s3_pipe_brands
    AUTO_INGEST = TRUE
    AS
    COPY INTO raw_data.brands
    FROM @raw_data.aws_ext_stage_integration
    PATTERN = '.*brand_data/.*\.json'
    FILE_FORMAT = (FORMAT_NAME = json_format);

-- creating the pipe for getting receipt_data
CREATE OR REPLACE PIPE aws_s3_pipe_receipts
    AUTO_INGEST = TRUE
    AS
    COPY INTO raw_data.receipts
    FROM @raw_data.aws_ext_stage_integration
    PATTERN = '.*receipt_data/.*\.json'
    FILE_FORMAT = (FORMAT_NAME = json_format);

-- getting the notification channel to use in SQS
SHOW PIPES;

-- checking if the external stage object is working
LIST @raw_data.aws_ext_stage_integration;

-- testing with select qerry after adding files to S3
SELECT * FROM raw_data.brands;

-- checking for notification from SQS
SELECT SYSTEM$PIPE_STATUS ('aws_s3_pipe_brands');