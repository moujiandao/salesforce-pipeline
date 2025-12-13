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
    "Account Source" AS account_source,
    CURRENT_TIMESTAMP() AS last_updated
FROM {{ source('raw', 'ACCOUNTS') }}