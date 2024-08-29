# From S3 to Redshift Data Pipeline (S3, Redshift, Airflow)
### Introduction
The music streaming company, Sparkify, has decided to use Apache Airflow to automate and monitor their ETL pipelines.

As a Data Engineer, my task is to create dynamic, high-grade data pipelines built from reusable tasks that can be monitored. Additionally, I'm responsible for running tests on the datasets after executing the ETL steps to capture any discrepancies.

Sparkify requires that the source data resides in S3 and the source datasets consist of CSV logs detailing user activity in the application and JSON metadata about songs listened to by users.
### Steps of Project
#### 1. Creation of Virtual Environment
Created a virtual environment in WSL. It will keep its libraries and dependencies separate from the global and any other project libraries to avoid any conflict between them.
#### 2. Designed Data Pipeling
2.1 Loading data from S3 to Staging tables in Redshift.<br>
2.2 Loading data from staging tables to Fact table.<br>
2.3 Loading data from staging tables to Dimension table.<br>
2.4 Performing data quality checks.<br>

### Technologies Used
Python <br>
configparser <br>
pyspark <br>
datetime <br>
S3 Buckets <br>
Redshift <br>
Airflow <br>
