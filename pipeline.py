import pandas as pd
import pyodbc
import logging
from azure.storage.blob import BlobServiceClient
import io

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ])
log = logging.getLogger(__name__)

# Azure Storage
import os
STORAGE_CONN_STR = os.environ.get("STORAGE_CONN_STR", "")

CONTAINER_NAME = "pricing-data"
BLOB_NAME = "pricing_data.csv"

# Azure SQL
SQL_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=optum-sql-server.database.windows.net;"
    "DATABASE=PricingDB;"
    "Authentication=ActiveDirectoryInteractive;"
    "UID=virat183672@gmail.com;"
)

def extract_from_blob():
    log.info("=== EXTRACT: Reading from Azure Blob Storage ===")
    client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
    blob = client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
    data = blob.download_blob().readall()
    df = pd.read_csv(io.BytesIO(data))
    log.info(f"Extracted {len(df)} rows from blob")
    return df

def transform(df):
    log.info("=== TRANSFORM: Cleaning and validating data ===")

    # Null check
    nulls = df.isnull().sum().sum()
    log.info(f"Nulls found: {nulls}")
    df = df.dropna()

    # Duplicate check
    dupes = df.duplicated().sum()
    log.info(f"Duplicates found: {dupes}")
    df = df.drop_duplicates()

    # Data quality checks
    df = df[df['BasePrice'] > 0]
    df = df[df['NegotiatedPrice'] > 0]
    df = df[df['Quantity'] > 0]
    df = df[df['NegotiatedPrice'] < df['BasePrice'] * 1.5]

    # Add derived column
    df['DiscountPct'] = round(
        (df['BasePrice'] - df['NegotiatedPrice']) / df['BasePrice'] * 100, 2
    )
    df['TxnDate'] = pd.to_datetime(df['TxnDate'])
    df['LoadedAt'] = pd.Timestamp.now()

    log.info(f"After transform: {len(df)} rows, {len(df.columns)} columns")
    return df

def load_to_azure_sql(df):
    log.info("=== LOAD: Inserting into Azure SQL ===")
    conn = pyodbc.connect(SQL_CONN_STR)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='PricingPipeline' AND xtype='U')
        CREATE TABLE PricingPipeline (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            ProductName NVARCHAR(200),
            Category NVARCHAR(100),
            BasePrice DECIMAL(10,2),
            CustomerName NVARCHAR(200),
            Tier NVARCHAR(50),
            Region NVARCHAR(100),
            NegotiatedPrice DECIMAL(10,2),
            DiscountPct DECIMAL(5,2),
            Quantity INT,
            TxnDate DATE,
            LoadedAt DATETIME
        )
    """)
    conn.commit()

    # Bulk insert
    cursor.fast_executemany = True
    sql = """INSERT INTO PricingPipeline
        (ProductName,Category,BasePrice,CustomerName,Tier,Region,
         NegotiatedPrice,DiscountPct,Quantity,TxnDate,LoadedAt)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)"""

    data = [tuple(row) for row in df[[
        'ProductName','Category','BasePrice','CustomerName','Tier','Region',
        'NegotiatedPrice','DiscountPct','Quantity','TxnDate','LoadedAt'
    ]].itertuples(index=False)]

    cursor.executemany(sql, data)
    conn.commit()
    conn.close()
    log.info(f"Loaded {len(df)} rows into Azure SQL PricingPipeline table")

def run_pipeline():
    log.info("========== PIPELINE STARTED ==========")
    df = extract_from_blob()
    df = transform(df)
    load_to_azure_sql(df)
    log.info("========== PIPELINE COMPLETE ==========")

if __name__ == "__main__":
    run_pipeline()