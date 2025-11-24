import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from pathlib import Path

print("loading .env...")
load_dotenv()

# Connect to Snowflake
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema='ANALYTICS' #hardcoded because .env is shared by other scripts
)
print("✓ Connected\n")

# Generate dates from 2020 to 2030 (adjust as needed)
start_date = datetime(2020, 1, 1)
end_date = datetime(2030, 12, 31)

dates = []
current_date = start_date

while current_date <= end_date:
    # Calculate fiscal year (assuming Feb 1 start - adjust for your company)
    if current_date.month >= 2:
        fiscal_year = current_date.year
    else:
        fiscal_year = current_date.year - 1
    
    # Calculate fiscal quarter
    fiscal_month = ((current_date.month - 2) % 12) + 1
    fiscal_quarter = ((fiscal_month - 1) // 3) + 1
    
    # Check if month end
    next_day = current_date + timedelta(days=1)
    is_month_end = next_day.month != current_date.month
    
    date_record = {
        'date_key': int(current_date.strftime('%Y%m%d')),
        'date': current_date.date(),
        'year': current_date.year,
        'quarter': (current_date.month - 1) // 3 + 1,
        'month': current_date.month,
        'month_name': current_date.strftime('%B'),
        'week_of_year': current_date.isocalendar()[1],
        'day_of_month': current_date.day,
        'day_of_week': current_date.isoweekday(),
        'day_name': current_date.strftime('%A'),
        'is_weekend': current_date.isoweekday() in [6, 7],
        'is_month_end': is_month_end,
        'fiscal_year': fiscal_year,
        'fiscal_quarter': fiscal_quarter
    }
    
    dates.append(date_record)
    current_date += timedelta(days=1)

# Create DataFrame and load to Snowflake
df = pd.DataFrame(dates)

cursor = conn.cursor()
print("Truncating dim_date...")

cursor.execute("TRUNCATE TABLE dim_date")
print("Truncate complete!")

# Insert records
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO dim_date VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """, tuple(row))

conn.commit()
cursor.close()
conn.close()

print(f"Loaded {len(df)} dates into dim_date")