# Sparkify Data Pipeline

**This project is an ETL pipeline orchestrated with Airflow and designed to store data in AWS Redshift.**

## Table of Contents

- [Prerequisites](#prerequisites)
- [Thought Process](#thought-process)


## Prerequisites

1. This project is configured to work within Udacity's workspace. To run it locally, you'll need to configure Airflow and adjust settings for your AWS environment (bucket, Redshift cluster, database).

2. Configurations to start the Airflow cluster can be seen on start.sh file.

3. Connections and variables need your own bucket, redshift cluster and database configurated accordingly.

## Thought process

1. StageToRedshiftOperator
    This operator copies a set of json files from an s3 path to a Redhshift table.
    I choose to input only json files to write the FORMAT AS JSON on the copy statement.
    The s3 paths are needed, an optional json formatting file is available.
    As staging tables are temporary, the tables are dropped before the new copy runs.
    To parametrize the backfills or partitions, we can use the DAG script to input informations from execution date, for example.

2. LoadFactOperator
    The fact tables are created from an existing table, with queries provided in an helper .sql file. 
    If an create table query is provided, the operator checks if the table already exists. If not, then this query is used.
    After that, data is inserted appending data.

3. LoadDimensionOperator
    The dimension tables are created using the same logic as fact tables, unless an additional argument mode="overwrite" is passed, then the table is dropped before inserting new files.

4. DataQualityOperator
    This operator was created to confirm that all prior steps were executed successfuly. 
    The checks are made on staging tables, to see only if records and primary ids exist.
    The input is a dictionary that contains table names and columns that shouldn't have null values. Like the following example:
    tables_and_columns= [
            {'table': 'table1', 'columns': ['column1', 'column2']},
            {'table': 'table2', 'columns': ['column1', 'column10']},
        ]
    In this configuration, only one task is needed for all tables used in the ETL.