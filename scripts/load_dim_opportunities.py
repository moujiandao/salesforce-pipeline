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



def load_dim_opportunities():
    conn = get_connection()
    cursor = conn.cursor()
    
    merge_sql = """
    MERGE INTO ANALYTICS.DIM_OPPORTUNITIES AS target
    USING (
        SELECT 
            "Opportunity ID" AS opportunity_id,
            "Opportunity Name" AS opportunity_name,
            "Account ID" AS account_id,
            "Stage" AS stage,
            "Amount (USD)" AS amount_usd,
            "Close Date" AS close_date,
            "Probability (%)" AS probability,
            "Lead Source" AS lead_source,
            "Description" AS description,
            "Created Date" AS created_date,
            "Owner" AS owner

        FROM RAW.OPPORTUNITIES
    ) AS source
    ON target.opportunity_id = source.opportunity_id
    
    WHEN MATCHED THEN UPDATE SET
        opportunity_name = source.opportunity_name,
        account_id = source.account_id,
        stage = source.stage,
        amount_usd = source.amount_usd,
        close_date = source.close_date,
        probability = source.probability,
        lead_source = source.lead_source,
        description = source.description,
        created_date = source.created_date,
        owner = source.owner,

        last_updated = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN INSERT (
        opportunity_id, opportunity_name, account_id, stage,
        amount_usd, close_date, probability, lead_source,
        description, created_date, owner, last_updated
    ) VALUES (
        source.opportunity_id, source.opportunity_name, source.account_id, source.stage,
        source.amount_usd, source.close_date, source.probability, source.lead_source,
        source.description, source.created_date, source.owner, CURRENT_TIMESTAMP()
    );
    """
    
    cursor.execute(merge_sql)
    
    cursor.execute("SELECT COUNT(*) FROM ANALYTICS.DIM_OPPORTUNITIES")
    count = cursor.fetchone()[0]
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"dim_opportunities loaded: {count} total records")

if __name__ == "__main__":
    load_dim_opportunities()