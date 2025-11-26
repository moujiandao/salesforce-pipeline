# Salesforce to S3 Data Pipeline

automated ELT pipeline that extracts SFDC data and loads it into AWS S3 for analytics.

##Overview

This project demonstrates data engineering best practices per the following:
1. Establishing connection to Salesforce via Salesforce API
2. Extracting SFDC Objects such as Accounts, Opportunities, etc. into S3
3. Conducting transformations on the raw S3 data and creating/updating the data in their respective Snowflake analytics tables. Transforming the data into formats such as the star schema (Facts and Dimensions) for query performance.
4. ...

## Tech Stack

- **Python 3**
- **Salesforce**
- **Boto3**
- **Pandas**
- **dbt**
- **AWS S3**
- **Snowflake datawarehouse**
