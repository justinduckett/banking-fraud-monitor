import pandas as pd
import random
from faker import Faker
from datetime import datetime
import time

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
        "customer_id": random.randint(1001, 1020), # Simulating a small user base of 20 people
        "merchant_id": random.randint(500, 550),
        "timestamp": datetime.now(), # Uses current time for "fresh" data
        "category": random.choice(['food', 'transport', 'retail', 'electronics', 'travel']),
    }
    
    # FRAUD INJECTION LOGIC
    # We intentionally create patterns we can detect with SQL later
    if is_fraud:
        # Pattern 1: High Value amount (e.g., $9,000 for electronics)
        transaction['amount'] = round(random.uniform(1000, 9999), 2)
        transaction['is_suspicious_flag'] = True # Helper for us to check accuracy later
        transaction['category'] = 'electronics' # Fraudsters love resaleable items
    else:
        # Normal spending pattern
        transaction['amount'] = round(random.uniform(5.00, 150.00), 2)
        transaction['is_suspicious_flag'] = False

    return transaction

def main():
    print(f"Generating {NUM_TRANSACTIONS} transactions...")
    
    data = [generate_transaction() for _ in range(NUM_TRANSACTIONS)]
    df = pd.DataFrame(data)
    
    # Save locally to check our work
    filename = f"transactions_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(filename, index=False)
    
    print(f"Success! Saved {len(df)} rows to {filename}")
    print(df.head())

if __name__ == "__main__":
    main()