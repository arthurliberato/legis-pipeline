# scripts/load_to_snowflake.py

import os
import pandas as pd
import snowflake.connector

# Get credentials from GitHub Actions environment
account = os.environ["SNOWFLAKE_ACCOUNT"]
user = os.environ["SNOWFLAKE_USER"]
password = os.environ["SNOWFLAKE_PASSWORD"]
database = os.environ["SNOWFLAKE_DATABASE"]
schema = os.environ["SNOWFLAKE_SCHEMA"]
warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=account,
    user=user,
    password=password,
    database=database,
    schema=schema,
    warehouse=warehouse
)

print("[CONNECT] Connected to Snowflake")

# Load the attendance data
csv_path = "processed_data/dominican_republic_attendance_2025.csv"
df = pd.read_csv(csv_path)

# Upload data
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS attendance (
        name STRING,
        status STRING,
        arrival_time STRING,
        session_date DATE,
        source_file STRING
    )
""")
print("[TABLE] Ensured 'attendance' table exists")

# Insert data
df["session_date"] = pd.to_datetime(df["session_date"], dayfirst=True).dt.date
df = df.where(pd.notnull(df), None)

for _, row in df.iterrows():
    cursor.execute(
        "INSERT INTO attendance (name, status, arrival_time, session_date, source_file) VALUES (%s, %s, %s, %s, %s)",
        (
            row["name"],
            row["status"],
            row.get("arrival_time", None),
            row.get("session_date", None),
            row.get("source_file", None)
        )
    )

print(f"[INSERT] Inserted {len(df)} rows")
cursor.close()
conn.close()