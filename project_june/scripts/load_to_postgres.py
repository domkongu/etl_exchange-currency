import psycopg2
import pandas as pd
import os

# Environment variables
DB_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
DB_NAME = os.environ.get('POSTGRES_DB', 'airflow')
DB_USER = os.environ.get('POSTGRES_USER', 'airflow')
DB_PASS = os.environ.get('POSTGRES_PASSWORD', 'airflow')
CSV_FILE = '/opt/airflow/scripts/exchange_rates.csv'

# Read CSV
df = pd.read_csv(CSV_FILE)

# Create table with fixed schema
create_table_sql = """
CREATE TABLE IF NOT EXISTS exchange_rates (
    date DATE NOT NULL,
    base_currency VARCHAR(10) NOT NULL,
    timestamp BIGINT,
    rate_per_VND FLOAT,
    PRIMARY KEY (date, base_currency)
)
"""

# Connect to DB
conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)
cur = conn.cursor()
cur.execute(create_table_sql)

# Prepare UPSERT statement
columns = ['date', 'base_currency', 'timestamp', 'rate_per_VND']
placeholders = ', '.join(['%s'] * len(columns))
update_assignments = ', '.join([f"{col} = EXCLUDED.{col}" for col in ['timestamp', 'rate_per_VND']])

insert_sql = f"""
INSERT INTO exchange_rates ({', '.join(columns)})
VALUES ({placeholders})
ON CONFLICT (date, base_currency)
DO UPDATE SET {update_assignments}
"""

# Handle NaNs
df = df.where(pd.notnull(df), None)

# Insert rows
for _, row in df.iterrows():
    values = [row[col] for col in columns]
    cur.execute(insert_sql, values)

# Commit and close
conn.commit()
cur.close()
conn.close()
