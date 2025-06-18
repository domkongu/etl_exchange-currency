import psycopg2
import pandas as pd
import os

DB_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
DB_NAME = os.environ.get('POSTGRES_DB', 'airflow')
DB_USER = os.environ.get('POSTGRES_USER', 'airflow')
DB_PASS = os.environ.get('POSTGRES_PASSWORD', 'airflow')
CSV_FILE = '/opt/airflow/scripts/exchange_rates.csv'

df = pd.read_csv(CSV_FILE)

# Dynamically create columns for all rate columns
def get_rate_columns(df):
    return [col for col in df.columns if col.startswith('rate_')]

rate_columns = get_rate_columns(df)

# Build CREATE TABLE statement
table_columns = [
    'date DATE',
    'base_currency VARCHAR(10)',
    'timestamp BIGINT'
] + [f'{col} FLOAT' for col in rate_columns]

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS exchange_rates (
    {', '.join(table_columns)}
)
"""

conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)
cur = conn.cursor()
cur.execute(create_table_sql)

# Prepare insert statement
df = df.where(pd.notnull(df), None)  # Replace NaN with None for SQL
columns = ['date', 'base_currency', 'timestamp'] + rate_columns
insert_sql = f"INSERT INTO exchange_rates ({', '.join(columns)}) VALUES ({', '.join(['%s']*len(columns))})"

for _, row in df.iterrows():
    values = [row[col] for col in columns]
    cur.execute(insert_sql, values)

conn.commit()
cur.close()
conn.close() 