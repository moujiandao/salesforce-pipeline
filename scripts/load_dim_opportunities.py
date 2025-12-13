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


def load_fact_opportunities():
    conn = get_connection()
    cursor = conn.cursor()
    
    merge_sql = """
    MERGE INTO ANALYTICS.FACT_OPPORTUNITIES AS target
    USING (
        SELECT 
            "Opportunity ID" AS opportunity_id,
            "Opportunity Name" AS opportunity_name,
            "Account ID" AS account_id,
            "Stage" AS stage,
            "Amount (USD)" AS amount,
            "Close Date" AS close_date,
            TO_NUMBER(TO_CHAR(TO_DATE("Close Date"), 'YYYYMMDD')) AS close_date_key,
            "Probability (%)" AS probability,
            "Lead Source" AS lead_source,
            "Created Date" AS created_date,
            TO_NUMBER(TO_CHAR(TO_DATE("Created Date"), 'YYYYMMDD')) AS created_date_key,
            "Owner" AS owner
        FROM RAW.OPPORTUNITIES
    ) AS source
    ON target.opportunity_id = source.opportunity_id
    
    WHEN MATCHED THEN UPDATE SET
        opportunity_name = source.opportunity_name,
        account_id = source.account_id,
        stage = source.stage,
        amount = source.amount,
        close_date = source.close_date,
        close_date_key = source.close_date_key,
        probability = source.probability,
        lead_source = source.lead_source,
        created_date = source.created_date,
        created_date_key = source.created_date_key,
        owner = source.owner
    
    WHEN NOT MATCHED THEN INSERT (
        opportunity_id, opportunity_name, account_id, stage,
        amount, close_date, close_date_key, probability, lead_source,
        created_date, created_date_key, owner
    ) VALUES (
        source.opportunity_id, source.opportunity_name, source.account_id, source.stage,
        source.amount, source.close_date, source.close_date_key, source.probability, 
        source.lead_source, source.created_date, source.created_date_key, source.owner
    );
    """
    
    cursor.execute(merge_sql)
    
    cursor.execute("SELECT COUNT(*) FROM ANALYTICS.FACT_OPPORTUNITIES")
    count = cursor.fetchone()[0]
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"fact_opportunities loaded: {count} total records")


if __name__ == "__main__":
    load_fact_opportunities()