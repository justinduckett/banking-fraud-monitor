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
    Generates a transaction for 'Now', but with a random time offset 
    within the last 24 hours to simulate a full day of activity.
    """
    is_fraud = random.random() < FRAUD_RATIO

    # LOGIC CHANGE:
    # Instead of exactly "now" (which clusters data),
    # we pick a random second in the last 24 hours.
    seconds_in_day = 24 * 60 * 60
    random_second = random.randint(0, seconds_in_day)
    fake_time = datetime.now() - pd.Timedelta(seconds=random_second)

    transaction = {
        "transaction_id": fake.uuid4(),
        "customer_id": random.randint(1001, 1020),
        "merchant_id": random.randint(500, 550),
        "timestamp": fake_time,
        "category": random.choice(['groceries', 'restaurants', 'entertainment', 'transport', 'retail', 'electronics', 'travel', 'home_improvement', 'clothing']),
    }

    # Fraud Logic (Keep this the same)
    if is_fraud:
        transaction['amount'] = round(random.uniform(1000, 9999), 2)
        transaction['is_suspicious_flag'] = True
        transaction['category'] = random.choice(
            ['electronics', 'gift_cards', 'crypto', 'online_gaming', 'cash_advance'])
    else:
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
