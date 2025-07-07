import os
import re
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import duckdb

# Load environment variables
load_dotenv()

# Constants
MYSQL_HOST = "database-cluster.cluster-ro-abcd1234.us-east-1.rds.amazonaws.com"
MYSQL_PORT = 3306
GCS_PARQUET_PATH = 'gs://onfly-storage-tables/tables/billing_status_summary/*.parquet'
GCS_OUTPUT_PATH = 'gs://onfly-storage-tables/tables/billing_status_summary/billing_status_summary.parquet'

# Authenticate GCS using HMAC keys
duckdb.sql(f"""
    CREATE SECRET IF NOT EXISTS(
        TYPE gcs,
        KEY_ID '{os.environ["HMAC_KEY_ID"]}',
        SECRET '{os.environ["HMAC_SECRET"]}'
    );
""")

# Read existing Parquet files if available
existing_files_df = duckdb.sql(f"SELECT * FROM glob('{GCS_PARQUET_PATH}')").df()

if existing_files_df.empty:
    duckdb.sql("""
        CREATE TABLE billing_status_summary_historical (
            invoice_id VARCHAR,
            company_id VARCHAR,
            company_name VARCHAR,
            billing_period VARCHAR,
            invoice_generation DATETIME,
            due_date DATETIME,
            payment_date DATETIME,
            status_invoice VARCHAR,
            status_process VARCHAR,
            payment_type VARCHAR,
            amount FLOAT,
            updated_at TIMESTAMP,
            inserted_at TIMESTAMP
        );
    """)
    updated_at_filter = ""
else:
    latest_updated_at = duckdb.sql(f"""
        SELECT MAX(updated_at) as max_updated
        FROM read_parquet('{GCS_PARQUET_PATH}')
    """).df().iloc[0, 0]

    updated_at_filter = f"AND payment_invoice.updated_at > '{latest_updated_at}'"

    historical_df = duckdb.sql(f"""
        SELECT * FROM read_parquet('{GCS_PARQUET_PATH}')
    """).df()

    duckdb.register("historical_view", historical_df)
    duckdb.sql("""
        CREATE TABLE IF NOT EXISTS billing_status_summary_historical as
        SELECT * FROM historical_view;
    """)

# MySQL connection
mysql_engine = create_engine(
    f'mysql+pymysql://{os.environ["MYSQL_DB_USER"]}:{os.environ["MYSQL_DB_PASSWORD"]}@{MYSQL_HOST}:{MYSQL_PORT}'
)

# Define the SQL query
billing_query = text(f"""
    SELECT DISTINCT *
    FROM (
        SELECT 
            companies.nome as company_name,
            CAST(companies.id as CHAR) as company_id,
            payment_invoice.created_at as invoice_generation,
            payment_invoice.due_date,
            CASE
                WHEN payment_invoice.status = 'paid' THEN payment_invoice.updated_at
                ELSE NULL
            END as payment_date,
            description,
            payment_invoice.status as status_invoice,
            payment_invoice.process as status_process,
            FORMAT(payment_invoice.amount / 100, 2) as amount,
            CASE
                WHEN payment_invoice.type = 1 THEN 'Boleto'
                WHEN payment_invoice.type = 2 THEN 'Cartão de Crédito'
                ELSE ''
            END as payment_type,
            payment_invoice.id as invoice_id,
            payment_invoice.updated_at
        FROM 
            onfly.payment_invoice
        LEFT JOIN onfly.companies 
            ON payment_invoice.company_id = companies.id
        WHERE 
            deleted_at IS NULL
            {updated_at_filter}
        ORDER BY payment_invoice.due_date
    ) as billing_data
""")

# Read new invoice records
new_invoice_df = pd.read_sql_query(billing_query, mysql_engine)

# Normalize and transform data
new_invoice_df["description_clean"] = new_invoice_df["description"].str.replace(r"\s+", " ", regex=True)

new_invoice_df['billing_period'] = (
    new_invoice_df["description"]
        .str
        .replace(r"\s+", " ", regex=True)
        .apply(
            lambda x: re.findall(r'\d{2}/\d{2}/\d{4}', x)[0] + " - " + re.findall(r'\d{2}/\d{2}/\d{4}', x)[1]
            if len(re.findall(r'\d{2}/\d{2}/\d{4}', x)) > 1 else ''
        )
    )

new_invoice_df["company_name"] = (
    new_invoice_df["company_name"]
    .str.replace(r"[^A-Za-zÀ-ÿ ]+", "", regex=True)
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
)

new_invoice_df["status_process"] = new_invoice_df["status_process"].fillna('')

# Type casting and cleanup
new_invoice_df["invoice_id"]         = new_invoice_df["invoice_id"].astype(str)
new_invoice_df["company_id"]         = new_invoice_df["company_id"].astype(str)
new_invoice_df["company_name"]       = new_invoice_df["company_name"].astype(str)
new_invoice_df["billing_period"]     = new_invoice_df["billing_period"].astype(str)
new_invoice_df["invoice_generation"] = pd.to_datetime(new_invoice_df["invoice_generation"]).dt.tz_localize('UTC')
new_invoice_df["due_date"]           = pd.to_datetime(new_invoice_df["due_date"]).dt.tz_localize('UTC')
new_invoice_df["payment_date"]       = pd.to_datetime(new_invoice_df["payment_date"]).dt.tz_localize('UTC')
new_invoice_df["status_invoice"]     = new_invoice_df["status_invoice"].astype(str)
new_invoice_df["status_process"]     = new_invoice_df["status_process"].astype(str)
new_invoice_df["payment_type"]       = new_invoice_df["payment_type"].astype(str)
new_invoice_df["amount"]             = pd.to_numeric(new_invoice_df["amount"].str.replace(",", ""), downcast="float").round(2)
new_invoice_df["updated_at"]         = pd.to_datetime(new_invoice_df["updated_at"]).dt.tz_localize('UTC')
new_invoice_df["inserted_at"]        = datetime.now(timezone.utc)


# Final columns
final_columns = [
    "invoice_id", 
    "company_id", 
    "company_name", 
    "billing_period",
    "invoice_generation", 
    "due_date", 
    "payment_date",
    "status_invoice", 
    "status_process", 
    "payment_type",
    "amount", 
    "updated_at", 
    "inserted_at"
]

final_invoice_df = new_invoice_df[final_columns]

if final_invoice_df.empty:
    print("No new invoices to process.")
else:
    # Register DataFrame in DuckDB
    duckdb.register("new_invoice_view", final_invoice_df)
    duckdb.sql("""
        CREATE TABLE IF NOT EXISTS billing_status_summary_new as
        SELECT * FROM new_invoice_view;
    """)

    # Perform UPSERT (merge by invoice_id)
    duckdb.sql("""
        UPDATE billing_status_summary_historical as target
        SET 
            invoice_generation = source.invoice_generation,
            due_date           = source.due_date,
            payment_date       = source.payment_date,
            status_invoice     = source.status_invoice,
            status_process     = source.status_process,
            payment_type       = source.payment_type,
            amount             = source.amount,
            updated_at         = source.updated_at,
            inserted_at        = source.inserted_at
        FROM billing_status_summary_new as source
        WHERE source.invoice_id = target.invoice_id
        ;

        INSERT INTO billing_status_summary_historical
        SELECT source.*
        FROM billing_status_summary_new as source
            LEFT JOIN billing_status_summary_historical as target
                ON source.invoice_id = target.invoice_id
        WHERE target.invoice_id IS NULL
        ;
    """)

    # Export final result to GCS
    duckdb.sql(f"""
        COPY billing_status_summary_historical
        TO '{GCS_OUTPUT_PATH}'
        WITH (
            FORMAT parquet,
            OVERWRITE TRUE,
            COMPRESSION 'snappy'
        );
    """)
