SELECT 
    "Opportunity ID" AS opportunity_id,
    "Account ID" AS account_id,
    "Opportunity Name" AS opportunity_name,
    "Stage" AS stage,
    "Amount (USD)" AS amount,
    "Probability (%)" AS probability,
    "Lead Source" AS lead_source,
    "Close Date" AS close_date,
    TO_NUMBER(TO_CHAR("Close Date", 'YYYYMMDD')) AS close_date_key,
    "Created Date" AS created_date,
    TO_NUMBER(TO_CHAR("Created Date", 'YYYYMMDD')) AS created_date_key,
    "Owner" AS owner
FROM {{ source('raw', 'OPPORTUNITIES') }}