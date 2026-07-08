import os
import random
import logging
from datetime import datetime

import pandas as pd
import pandas_gbq
from faker import Faker
from google.oauth2 import service_account

# Logging gives us timestamped output in the GitHub Actions logs,
# which makes it much easier to diagnose failed runs than bare print()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

fake = Faker()

# --- Configuration ---
# Values come from environment variables when available (how the GitHub
# Actions workflow runs it), with sensible defaults for local runs.
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "banking-fraud-monitor")
TABLE_ID = os.environ.get("BQ_TABLE_ID", "fraud_data.raw_transactions")
CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")

NUM_TRANSACTIONS = int(os.environ.get("NUM_TRANSACTIONS", 100))
FRAUD_RATIO = 0.05        # 5% of normal traffic is a lone high-amount fraud
BURST_PROBABILITY = 0.4   # 40% chance per run that one customer suffers a velocity attack

SECONDS_IN_DAY = 24 * 60 * 60

NORMAL_CATEGORIES = [
    'groceries', 'restaurants', 'entertainment', 'transport', 'retail',
    'electronics', 'travel', 'home_improvement', 'clothing',
]
FRAUD_CATEGORIES = ['electronics', 'gift_cards', 'crypto', 'online_gaming', 'cash_advance']


def random_time_last_24h():
    """A random moment in the last 24 hours, so daily data doesn't cluster at run time."""
    return datetime.now() - pd.Timedelta(seconds=random.randint(0, SECONDS_IN_DAY))


def generate_transaction():
    """One transaction. A small share are lone high-amount frauds (spend spikes)."""
    is_fraud = random.random() < FRAUD_RATIO

    transaction = {
        "transaction_id": fake.uuid4(),
        "customer_id": random.randint(1001, 1020),
        "merchant_id": random.randint(500, 550),
        "timestamp": random_time_last_24h(),
    }

    if is_fraud:
        # Fraud signature 1: a single transaction far above the user's normal spend
        transaction["amount"] = round(random.uniform(1000, 9999), 2)
        transaction["category"] = random.choice(FRAUD_CATEGORIES)
        transaction["is_suspicious_flag"] = True
    else:
        transaction["amount"] = round(random.uniform(5.00, 150.00), 2)
        transaction["category"] = random.choice(NORMAL_CATEGORIES)
        transaction["is_suspicious_flag"] = False

    return transaction


def generate_burst(customer_id):
    """
    Fraud signature 2: a velocity attack (card testing).

    A fraudster validating a stolen card makes many small purchases in rapid
    succession. We cluster 5 to 9 small transactions for one customer inside
    a ~45 minute window, so the velocity_last_hour feature in dbt has a real
    pattern to catch (>3 transactions per hour).
    """
    burst_size = random.randint(5, 9)
    # Start the burst at least an hour before "now" so the whole burst
    # lands inside the last 24 hours
    burst_start = datetime.now() - pd.Timedelta(seconds=random.randint(3600, SECONDS_IN_DAY))

    transactions = []
    for _ in range(burst_size):
        transactions.append({
            "transaction_id": fake.uuid4(),
            "customer_id": customer_id,
            "merchant_id": random.randint(500, 550),
            "timestamp": burst_start + pd.Timedelta(seconds=random.randint(0, 2700)),
            "category": random.choice(FRAUD_CATEGORIES),
            "amount": round(random.uniform(0.50, 15.00), 2),
            "is_suspicious_flag": True,
        })
    return transactions


def main():
    log.info("Generating %s baseline transactions...", NUM_TRANSACTIONS)
    data = [generate_transaction() for _ in range(NUM_TRANSACTIONS)]

    # Occasionally inject a velocity attack on one unlucky customer
    if random.random() < BURST_PROBABILITY:
        victim = random.randint(1001, 1020)
        burst = generate_burst(victim)
        data.extend(burst)
        log.info("Injected a velocity attack: %s rapid transactions for customer %s",
                 len(burst), victim)

    df = pd.DataFrame(data)

    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)

    log.info("Uploading %s rows to %s.%s ...", len(df), PROJECT_ID, TABLE_ID)
    try:
        pandas_gbq.to_gbq(
            df,
            destination_table=TABLE_ID,
            project_id=PROJECT_ID,
            credentials=credentials,
            if_exists="append",
        )
        log.info("Success. Data uploaded to BigQuery.")
    except Exception:
        log.exception("Upload to BigQuery failed.")
        # Re-raise so the process exits nonzero and GitHub Actions marks
        # the run as failed. A silent failure here means the dashboard
        # serves stale data with no warning.
        raise


if __name__ == "__main__":
    main()