# Banking Fraud Detection Pipeline

## Summary

An end-to-end analytics engineering project that simulates a real-time fraud detection engine. It ingests synthetic financial transaction data, processes it through a modern data stack, and serves operational alerts to a fraud investigation dashboard.

![Dashboard Screenshot](assets/dashboard-screenshot.png) _[Note: Take a screenshot of your Looker dashboard and add it here later]_

## Architecture & Tools

- **Ingestion:** Python (Faker, Pandas) with simulated "24-hour jitter" to mimic real-time transaction flow.
- **Warehouse:** Google BigQuery (Storage & Compute).
- **Transformation:** dbt Cloud (Modeling, Testing, Documentation).
- **Orchestration:** dbt Cloud Scheduler.
- **Visualization:** Looker Studio (Operational Dashboards).

## Key Features Implemented

This project was built to demonstrate specific requirements for the Senior Analytics Developer role:

- **Feature Engineering:** Implemented complex SQL Window Functions to calculate `velocity_last_hour` and `avg_spend_30_days`[cite: 15, 30].
- **Automated Testing:** Configured dbt tests to ensure data quality (unique IDs, non-negative amounts) and logical consistency (velocity >= 1)[cite: 18, 29].
- **Data Contracts:** Explicitly cast all upstream columns to strict data types (String/Numeric) to prevent downstream BI errors[cite: 25].
- **Risk Modeling:** Developed logic to flag "High Amount Spikes" (>5x user average) and "Velocity Attacks" (>3 transactions/hour)[cite: 10].

## How to Run

1.  **Setup:** Clone the repo and install dependencies: `pip install -r requirements.txt`
2.  **Generate Data:** Run `python generate_data.py` to push fresh transactions to BigQuery.
3.  **Transform:** Run `dbt run` to rebuild the feature store.
4.  **Test:** Run `dbt test` to validate data integrity.
