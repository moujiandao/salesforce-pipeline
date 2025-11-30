# Salesforce to S3 Data Pipeline

Automated ELT pipeline that extracts SFDC data and loads it into AWS S3 for analytics.

## Overview

This project demonstrates data engineering best practices per the following:
1. Establishing connection to Salesforce via Salesforce API
2. Extracting SFDC Objects such as Accounts, Opportunities, etc. into S3
3. Conducting transformations on the raw S3 data and creating/updating the data in their respective Snowflake analytics tables. Transforming the data into formats such as the star schema (Facts and Dimensions) for query performance.
4. ...

## Data Model

**dim_date**

**dim_account**

**dim_opportuntiies**


## Tech Stack

- **Python 3**
- **Salesforce**
- **Boto3**
- **Pandas**
- **dbt**
- **AWS S3**
- **Snowflake datawarehouse**

## Setup
1. Clone repo
2. Create your own '.env' file with credentials.
3. Run scripts in order

## Scripts (in order)
upload_to_s3.py </br>
upload_to_snowflake.py </br>
generate_dim_date.py </br>
load_dim_account.py </br>
load_fact_opportunities.py </br>
