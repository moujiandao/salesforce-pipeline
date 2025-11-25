import snowflake.connector
import configparser
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

credentials_path = Path.home() / '.aws' / 'credentials'
config = configparser.ConfigParser()
config.read(credentials_path)
aws_key_id = config['default']['aws_access_key_id']
aws_secret_key = config['default']['aws_secret_access_key']

print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema='RAW' #hardcoded because .env is shared by other scripts
)
cursor = conn.cursor()
print("✓ Connected\n")

cursor.execute("""
    CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    PARSE_HEADER = TRUE
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
""")

cursor.execute("""
    CREATE OR REPLACE FILE FORMAT CSV_FORMAT_LOAD
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
""")


bucket = os.getenv('S3_BUCKET')
cursor.execute(f"""
    CREATE OR REPLACE STAGE S3_STAGE
    URL = 's3://{bucket}/raw/salesforce/'
    CREDENTIALS = (
        AWS_KEY_ID = '{aws_key_id}'
        AWS_SECRET_KEY = '{aws_secret_key}'
    )
    FILE_FORMAT = CSV_FORMAT
""")

# Auto-create ACCOUNTS table from CSV
print("Creating ACCOUNTS table from CSV schema...")
cursor.execute("""
    CREATE OR REPLACE TABLE ACCOUNTS
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION => '@S3_STAGE/Salesforce_Accounts.csv',
                FILE_FORMAT => 'CSV_FORMAT'
            )
        )
    )
""")

# Auto-create OPPORTUNITIES table from CSV
print("Creating OPPORTUNITIES table from CSV schema...")
cursor.execute("""
    CREATE OR REPLACE TABLE OPPORTUNITIES
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION => '@S3_STAGE/Salesforce_Opportunities.csv',
                FILE_FORMAT => 'CSV_FORMAT'
            )
        )
    )
""")
print("✓ Tables created\n")

# Load Accounts
print("Loading Accounts...")
cursor.execute("""
    COPY INTO ACCOUNTS
    FROM @S3_STAGE/Salesforce_Accounts.csv
    FILE_FORMAT = CSV_FORMAT_LOAD
""")
result = cursor.fetchone()
print(f"  ✓ Rows loaded: {result[3]}\n")

# Load Opportunities
print("Loading Opportunities...")
cursor.execute("""
    COPY INTO OPPORTUNITIES
    FROM @S3_STAGE/Salesforce_Opportunities.csv
    FILE_FORMAT = CSV_FORMAT_LOAD
""")
result = cursor.fetchone()
print(f"  ✓ Rows loaded: {result[3]}\n")

# Final counts
print("="*50)
cursor.execute("SELECT COUNT(*) FROM ACCOUNTS")
print(f"Total Accounts: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM OPPORTUNITIES")
print(f"Total Opportunities: {cursor.fetchone()[0]}")
print("="*50)

cursor.close()
conn.close()