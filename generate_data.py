import pandas as pd
import random
from faker import Faker
from datetime import datetime
import time
from google.oauth2 import service_account
import pandas_gbq

# Initialize Faker
fake = Faker()

# Configuration
NUM_TRANSACTIONS = 100  # How many rows to generate per run
FRAUD_RATIO = 0.05      # 5% of transactions should be suspicious


def generate_transaction():
    """
    Generates a single transaction. 
    Most are normal, some are intentionally 'suspicious' to test our models later.
    """
    is_fraud = random.random() < FRAUD_RATIO

    transaction = {
        "transaction_id": fake.uuid4(),
        # Simulating a small user base of 20 people
        "customer_id": random.randint(1001, 1020),
        "merchant_id": random.randint(500, 550),
        "timestamp": datetime.now(),  # Uses current time for "fresh" data
        "category": random.choice(['food', 'transport', 'retail', 'electronics', 'travel']),
    }

    # FRAUD INJECTION LOGIC
    # We intentionally create patterns we can detect with SQL later
    if is_fraud:
        # Pattern 1: High Value amount (e.g., $9,000 for electronics)
        transaction['amount'] = round(random.uniform(1000, 9999), 2)
        # Helper for us to check accuracy later
        transaction['is_suspicious_flag'] = True
        # Fraudsters love resaleable items
        transaction['category'] = 'electronics'
    else:
        # Normal spending pattern
        transaction['amount'] = round(random.uniform(5.00, 150.00), 2)
        transaction['is_suspicious_flag'] = False

    return transaction


def main():
    print(f"Generating {NUM_TRANSACTIONS} transactions...")

    data = [generate_transaction() for _ in range(NUM_TRANSACTIONS)]
    df = pd.DataFrame(data)

    # 1. Define Authentication
    # This tells Python: "I am this Service Account, here is my ID."
    credentials = service_account.Credentials.from_service_account_file(
        'service_account.json',
    )

    # 2. Upload to BigQuery
    # Format: project_id.dataset_id.table_id
    project_id = 'banking-fraud-monitor'
    table_id = 'fraud_data.raw_transactions'

    print(f"Uploading {len(df)} rows to BigQuery...")

    try:
        pandas_gbq.to_gbq(
            df,  # The dataframe is now the first argument
            destination_table=table_id,
            project_id=project_id,
            credentials=credentials,
            if_exists='append'
        )
        print("Success! Data uploaded to BigQuery.")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
