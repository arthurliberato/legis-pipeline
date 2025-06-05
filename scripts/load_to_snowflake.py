import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# Optional for local testing; won't run in GitHub Actions
load_dotenv()

# Load credentials
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "LEGISDATA")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

# Load data
CSV_PATH = "processed_data/dominican_republic_attendance_2025.csv"
df = pd.read_csv(CSV_PATH)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    warehouse=SNOWFLAKE_WAREHOUSE
)

cursor = conn.cursor()

# Ensure table exists (basic structure)
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS RAW_ATTENDANCE (
        NAME STRING,
        STATUS STRING,
        ARRIVAL_TIME STRING,
        DATE STRING,
        SOURCE_FILE STRING
    )
""")

# Insert data
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO RAW_ATTENDANCE (NAME, STATUS, ARRIVAL_TIME, DATE, SOURCE_FILE)
        VALUES (%s, %s, %s, %s, %s)
    """, tuple(row.fillna("").values))

print(f"[SUCCESS] Uploaded {len(df)} rows to RAW_ATTENDANCE")

cursor.close()
conn.close()
 
