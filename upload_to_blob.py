import pandas as pd
from azure.storage.blob import BlobServiceClient
import io
import logging

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Azure Storage connection
STORAGE_CONN_STR = os.environ.get("STORAGE_CONN_STR", "")
CONTAINER_NAME = "pricing-data"

def generate_pricing_csv():
    log.info("Generating pricing data CSV...")
    import random
    from faker import Faker
    fake = Faker()
    random.seed(42)

    categories = ['Cholesterol','Diabetes','Blood Pressure','Antibiotic','Gastric']
    drugs = ['Lipitor','Metformin','Lisinopril','Atorvastatin','Amoxicillin']
    doses = ['5mg','10mg','20mg','50mg','100mg']

    rows = []
    for i in range(1000):
        rows.append({
            'ProductName': f"{random.choice(drugs)} {random.choice(doses)}",
            'Category': random.choice(categories),
            'BasePrice': round(random.uniform(5.0, 150.0), 2),
            'CustomerName': fake.company(),
            'Tier': random.choice(['Platinum','Gold','Silver']),
            'Region': random.choice(['Northeast','Southeast','Midwest','National']),
            'NegotiatedPrice': round(random.uniform(4.0, 140.0), 2),
            'Quantity': random.randint(10, 2000),
            'TxnDate': fake.date_between(start_date='-1y', end_date='today')
        })
    return pd.DataFrame(rows)

def upload_to_blob():
    log.info("Connecting to Azure Blob Storage...")
    client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)

    # Create container
    try:
        client.create_container(CONTAINER_NAME)
        log.info(f"Container '{CONTAINER_NAME}' created")
    except Exception:
        log.info(f"Container '{CONTAINER_NAME}' already exists")

    # Generate and upload CSV
    df = generate_pricing_csv()
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    blob_client = client.get_blob_client(
        container=CONTAINER_NAME,
        blob="pricing_data.csv"
    )
    blob_client.upload_blob(csv_data, overwrite=True)
    log.info(f"Uploaded {len(df)} rows to blob: pricing_data.csv")
    return len(df)

if __name__ == "__main__":
    upload_to_blob()