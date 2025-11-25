import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE')
    )

def load_dim_account():
    conn = get_connection()
    cursor = conn.cursor()
    
    merge_sql = """
    MERGE INTO ANALYTICS.DIM_ACCOUNT AS target
    USING (
        SELECT 
            "Account ID" AS account_id,
            "Account Name" AS account_name,
            "Type" AS account_type,
            "Industry" AS industry,
            "Annual Revenue" AS annual_revenue,
            "Billing City" AS billing_city,
            "Billing State" AS billing_state,
            "Billing Country" AS billing_country,
            "Ownership" AS ownership,
            "Rating" AS rating,
            "Number of Employees" AS number_of_employees,
            "Account Source" AS account_source
        FROM RAW.ACCOUNTS
    ) AS source
    ON target.account_id = source.account_id
    
    WHEN MATCHED THEN UPDATE SET
        account_name = source.account_name,
        account_type = source.account_type,
        industry = source.industry,
        annual_revenue = source.annual_revenue,
        billing_city = source.billing_city,
        billing_state = source.billing_state,
        billing_country = source.billing_country,
        ownership = source.ownership,
        rating = source.rating,
        number_of_employees = source.number_of_employees,
        account_source = source.account_source,
        last_updated = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN INSERT (
        account_id, account_name, account_type, industry,
        annual_revenue, billing_city, billing_state,
        billing_country, ownership, rating,
        number_of_employees, account_source
    ) VALUES (
        source.account_id, source.account_name, source.account_type, source.industry,
        source.annual_revenue, source.billing_city, source.billing_state,
        source.billing_country, source.ownership, source.rating,
        source.number_of_employees, source.account_source
    );
    """
    
    cursor.execute(merge_sql)
    
    cursor.execute("SELECT COUNT(*) FROM ANALYTICS.DIM_ACCOUNT")
    count = cursor.fetchone()[0]
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"dim_account loaded: {count} total records")

if __name__ == "__main__":
    load_dim_account()