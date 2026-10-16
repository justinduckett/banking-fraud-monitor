# Banking Fraud Detection Pipeline

An end to end analytics engineering project that simulates a real time fraud detection system. Synthetic banking transactions are generated and ingested daily, scored against fraud detection rules in a modern data stack, and served to an operational triage dashboard for fraud investigators.

**[View the live dashboard](https://lookerstudio.google.com/s/kWp8xAUTeTU)**


## Architecture

| Stage | Tool | Detail |
|---|---|---|
| Data generation | Python (Faker, pandas) | Simulates daily transactions with two embedded fraud patterns |
| Ingestion | GitHub Actions | Scheduled workflow loads new transactions to BigQuery each morning (07:23 UTC) |
| Warehouse | Google BigQuery | Raw append only log plus modeled layers |
| Transformation | dbt Cloud | Staging and mart models, tests, and generated documentation |
| Orchestration | GitHub Actions + dbt Cloud Scheduler | Ingestion runs first; the dbt job runs at 14:00 UTC with a deliberate buffer (see design notes) |
| Dashboard | Looker Studio | Two page operational report: daily triage feed and category pattern analysis |

## How fraud detection works

The pipeline engineers behavioral features for every transaction using SQL window functions, then applies two rules:

**High amount spike.** A transaction more than 5x the customer's average spend over the preceding 30 days. The baseline window deliberately excludes the current transaction so a large fraud cannot inflate the very average it is judged against:

```sql
avg(amount) over (
    partition by customer_id
    order by unix_seconds(transaction_at)
    range between 2592000 preceding and 1 preceding
) as avg_spend_30_days
```

**Velocity attack.** More than 3 transactions by one customer within a rolling hour, the signature of card testing, where a fraudster validates a stolen card with rapid small purchases. The data generator injects realistic attack bursts (5 to 9 small transactions clustered within a 45 minute window) so this rule has genuine patterns to detect.

Every transaction receives a `transaction_type` classification (normal, high_amount_spike, velocity_attack, or spike_and_velocity) computed in dbt. All thresholds and definitions live in version controlled, tested SQL rather than in the BI layer, so every consumer works from one set of definitions.

## Data quality and documentation

- dbt tests run on every scheduled build: primary key uniqueness, not null constraints, non negative amounts, logical checks (velocity is always at least 1), and an accepted values test that fails the build if the classification logic ever produces an unexpected category
- Every column in every model is documented in yml, generating a browsable data dictionary through dbt docs
- Explicit type casting in the staging layer acts as a data contract, preventing downstream BI errors
- Development and production environments are separated: experimental work builds to a dev schema and can never touch the live dashboard

## Design decisions and limitations

**Rules are a triage layer, not a verdict.** A legitimate laptop purchase can trip the spike rule and a mall trip can approach the velocity threshold. That is expected. Flagged transactions are prioritized for manual review, and the rules are deliberately tuned to over flag because a false positive costs an analyst a minute while a missed fraud costs the full loss. A production system would add more features (merchant risk, geography, distinct merchants per hour) and an ML scoring layer on top of this feature store. The dashboard's category mix chart makes both false positives and false negatives visible rather than hiding them.

**Two crons are not an orchestrator.** Ingestion and transformation are scheduled independently, sequenced by a six hour buffer rather than a true dependency. If ingestion fails or runs late, dbt still rebuilds on stale data and reports success. I hit this exact failure during development, when the dbt job ran before the day's ingestion and the dashboard silently served yesterday's data. Cron scheduling cannot express "run B only after A succeeds," which is precisely the problem dependency aware orchestrators like Airflow and Prefect exist to solve. Documenting the limitation was the right scope for this project.

**GitHub Actions disables idle schedules.** Scheduled workflows are automatically switched off after 60 days without repository activity. The dashboard now carries a data freshness indicator so staleness is visible at a glance, and the limitation is accepted as a fair tradeoff of free tier infrastructure.

**Synthetic data cuts both ways.** The category risk signal on the dashboard is stronger than real fraud data would show, because the simulator draws fraudulent transactions from a category list that normal transactions barely touch. The `is_suspicious_flag` column is a ground truth label used only to evaluate rule performance, never as a rule input. Real transaction data would not include it.

**Data arrives with a one day smear.** Each ingestion run spreads its transactions across the previous 24 hours to mimic a daily batch file, so the current day always appears partial until the following morning's run completes it.

## Lessons learned the hard way

- **Fail loudly.** The original ingestion script caught upload exceptions and exited cleanly, so failed runs showed green in GitHub Actions. It now re raises, turning failures red and triggering notification emails.
- **Rotate credentials with an inventory.** Rotating the BigQuery service account key broke dbt Cloud, because three systems held copies of the old key and only two were updated. Credential rotation starts with listing every consumer.
- **Monitor freshness, not just success.** A pipeline can be green and stale at the same time. The dashboard's latest transaction indicator exists because this project spent four months proving it.

## How to run

1. Clone the repo and create a virtual environment: `python -m venv venv && source venv/bin/activate`
2. Install dependencies: `pip install -r requirements.txt` (one dependency list serves both local development and CI)
3. Add a GCP service account key as `service_account.json` in the project root (never committed; see `.gitignore`)
4. Generate and load data: `python generate_data.py`
5. Transformations run in dbt Cloud against this repo; the daily job executes `dbt build`, which runs models and tests in dependency order

## Links

- [Live dashboard](https://lookerstudio.google.com/s/kWp8xAUTeTU)
- [Portfolio writeup](https://justinduckett.github.io/portfolio/)
