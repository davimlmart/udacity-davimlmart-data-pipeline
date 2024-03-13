#!/bin/bash
#
# TO-DO: run the follwing command and observe the JSON output: 
# airflow connections get aws_credentials -o json 
# 
# [{"id": "68", 
# "conn_id": "aws_credentials", 
# "conn_type": "aws", 
# "description": "", "host": "", 
# "schema": "", 
# "login": "", 
# "password": "", 
# "port": null, 
# "is_encrypted": "True", 
# "is_extra_encrypted": "True", 
# "extra_dejson": {}, 
# "get_uri": ""}]
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add aws_credentials --conn-uri ''
#
#
# TO-DO: run the follwing command and observe the JSON output: 
# airflow connections get redshift -o json
# 
# [{"id": "67", 
# "conn_id": "redshift", 
# "conn_type": "redshift", 
# "description": "", 
# "host": "default-workgroup.767398009433.us-west-2.redshift-serverless.amazonaws.com:5439/dev", 
# "schema": "dev", 
# "login": "awsuser", 
# "password": "", 
# "port": "5439", 
# "is_encrypted": "True", 
# "is_extra_encrypted": "True", 
# "extra_dejson": {}, 
# "get_uri": ""}]
#
# Copy the value after "get_uri":
#
# For example: redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add redshift --conn-uri ''
#
# TO-DO: update the following bucket name to match the name of your S3 bucket and un-comment it:
#
airflow variables set s3_bucket udacity-davimlmart-data-pipeline
#
# TO-DO: un-comment the below line:
#
airflow variables set s3_prefix data-pipelines