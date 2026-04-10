# FinFlow Analytics

An end-to-end data engineering pipeline simulating a fintech payments platform. Built to explore cloud data warehousing, automated transformation, data quality management, and self-service analytics.

**Stack:** Python · Snowflake · dbt Cloud · n8n · Power Automate · Power BI · GitHub

---

## What this project does

Generates synthetic fintech data with real-world anomalies, moves it through a medallion architecture in Snowflake, transforms it with dbt Cloud, orchestrates the pipeline with n8n, and delivers live KPI dashboards in Power BI.

---

## Architecture

```
Python (data generation)
        |
        v
Snowflake Bronze (raw tables)
        |
        v
dbt Cloud (10 SQL models)
        |              |
        v              v
Snowflake Silver    Snowflake Quarantine
(4 clean views)     (3 tables, bad rows)
        |
        v
Snowflake Gold (3 mart tables)
        |
        v
Power BI Dashboard (DirectQuery)
        |
        v
n8n Orchestration + Power Automate Refresh
```

---

## Data

Synthetic data generated with Python across 4 tables:

| Table | Rows | Description |
|-------|------|-------------|
| customers | 2,000 | Signups, acquisition channel, CAC |
| subscriptions | 2,007 | Plans, MRR, churn, upgrades |
| transactions | 10,000 | Payments, failures, refunds |
| monthly_revenue | 7,006 | Aggregated MRR per customer per month |

**Total: 21,013 records**

### Intentional data quality issues seeded

| Issue | Table | Approx count |
|-------|-------|-------------|
| Duplicate IDs | customers, subscriptions, transactions | ~20 each |
| NULL customer ID | transactions | ~300 |
| Negative amounts | transactions | ~200 |
| Mixed date formats | all tables | throughout |
| NULL plan | subscriptions | ~40 |
| Zero MRR | subscriptions | ~30 |
| Future-dated rows | transactions | ~50 |
| NULL email | customers | ~40 |

---

## Medallion Architecture

### Bronze layer: `finflow_bronze.raw`
Raw data loaded as-is. No transformations, no constraints. Mirrors the source exactly. Stored as tables.

### Silver layer: `finflow_silver.silver`
dbt staging models clean what can be fixed automatically:
- Type casting and column renaming
- Date standardisation (mixed formats resolved)
- Deduplication via `ROW_NUMBER()`
- Invalid value exclusion (negatives, nulls)

Stored as **views** so they always reflect the latest Bronze data with zero storage cost.

### Quarantine layer: `finflow_silver.quarantine`
Rows that cannot be fixed automatically are routed here. Stored as **incremental tables** so the history is preserved even after the source is corrected.

| Model | Issues captured |
|-------|----------------|
| quarantine_customers | Null IDs, bad dates, negative CAC, null email |
| quarantine_subscriptions | Null plan, zero MRR, null customer ID |
| quarantine_transactions | Null customer ID, negative/zero amounts |

**558 total records quarantined**

The fix cycle: data team investigates quarantine table, fixes the record at the source system, reloads Bronze, re-runs pipeline. The corrected row passes staging checks and flows to Silver and Gold automatically.

### Gold layer: `finflow_gold.gold`
Pre-aggregated data mart tables with predefined KPI definitions. Stored as **tables** for fast DirectQuery performance from Power BI.

| Mart | KPIs |
|------|------|
| mart_mrr | MRR, ARR, new MRR, expansion MRR, churned MRR, net new MRR by month and plan |
| mart_cac_ltv | CAC, LTV, LTV:CAC ratio, payback period by channel and plan |
| mart_transactions | Success rate, volume, failure rate by month, channel and payment method |

---

## dbt Models

```
models/
  staging/
    stg_customers.sql
    stg_subscriptions.sql
    stg_transactions.sql
    stg_monthly_revenue.sql
    schema.yml
  quarantine/
    quarantine_customers.sql
    quarantine_subscriptions.sql
    quarantine_transactions.sql
  marts/
    mart_mrr.sql
    mart_cac_ltv.sql
    mart_transactions.sql
  sources.yml
```

All models include an AI-generation comment block documenting the prompt used and validation steps taken:

```sql
-- AI-generated: Claude Sonnet, reviewed and validated by Precious Oben
-- Model: stg_customers
-- AI prompt used: "Write a dbt staging model that cleans the customers table..."
```

### dbt tests

Data quality tests defined in `schema.yml`:
- `unique` and `not_null` on all primary keys
- `not_null` with `severity: warn` on fields where nulls are expected to be quarantined

---

## Orchestration

### n8n Cloud
Workflow: **FinFlow Pipeline Orchestration**

```
Schedule Trigger (daily 06:00)
        |
        v
HTTP POST: trigger dbt Cloud job via API
        |
        v
Wait 30 seconds
        |
        v
HTTP GET: check job run status
        |
        v
IF status = success
   |              |
   v              v
No operation   Send Outlook alert
               (pipeline failed)
```

### Power Automate
Flow: **Finflow Dashboard Refresh**

Independent schedule: daily 06:00. Triggers Power BI dataset refresh via the Power BI connector.

Note: n8n and Power Automate run independently. If the dbt pipeline fails overnight, Power Automate still refreshes Power BI at 06:00 but the dashboard will show stale data. The team is notified via the n8n email alert.

---

## Dashboard

Three pages built in Power BI Desktop, connected to Snowflake Gold layer via DirectQuery.

**Page 1: Analytics**
- KPIs: Total MRR, ARR, Active Customers, Transaction Success Rate
- MRR trend (Jan to Dec 2024)
- MRR by plan (donut)
- New vs expansion vs churned MRR (stacked bar)
- Customers by country
- Data quality summary

**Page 2: Unit Economics**
- KPIs: Avg CAC, Avg LTV, LTV:CAC Ratio, Avg Payback Period
- CAC by acquisition channel
- LTV by plan
- LTV:CAC ratio by channel
- Unit economics health scorecard by plan

**Page 3: Transaction Intelligence**
- KPIs: Total Transactions, Success Rate, Successful Volume, Failure Rate
- Monthly success and failure rate trend
- Volume by payment method
- Success rate by channel
- Data quality quarantine breakdown




## Setup

### Prerequisites
- Snowflake account (free trial available)
- dbt Cloud account (free developer tier/free trial for the paid version for API linking)
- n8n Cloud account (free trial available)
- Power BI Desktop
- Python 3.8+

### Generate data

```bash
pip install pandas numpy
python generate_finflow_data.py
```

Output: `data/raw/` with 4 CSV files.

### Load to Snowflake

1. Create databases and warehouse:

```sql
CREATE DATABASE finflow_bronze;
CREATE DATABASE finflow_silver;
CREATE DATABASE finflow_gold;

CREATE WAREHOUSE finflow_wh
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

2. Create raw schema and tables in `finflow_bronze`
3. Stage and load CSVs using `COPY INTO`

### dbt Cloud setup

1. Connect dbt Cloud to this GitHub repo
2. Set Snowflake connection with account identifier, warehouse, and database
3. Create production environment: `finflow_production`
4. Create deploy job: `finflow_production_run` with commands `dbt run` and `dbt test`

### n8n setup

1. Create workflow: `FinFlow Pipeline Orchestration`
2. Add Schedule Trigger (daily 06:00)
3. Add HTTP Request node (POST to dbt Cloud API)
4. Add Wait node (30 seconds)
5. Add HTTP Request node (GET job status)
6. Add IF node (check status = 10 for success)
7. Add Outlook node on false branch for failure alert

### Power BI

1. Open Power BI Desktop
2. Get data: Snowflake connector
3. Connect to `finflow_gold.gold` schema
4. Select `MART_MRR`, `MART_CAC_LTV`, `MART_TRANSACTIONS`
5. Load as DirectQuery

---

## AI role in this project

Claude (Sonnet) was used to generate all 10 dbt SQL models based on schema descriptions and business rules. Every model was reviewed, tested against the raw data, and validated before deployment. dbt tests were used to catch edge cases.

This reduced transformation development time significantly while maintaining full ownership of data quality and business logic.

---

## Author

**Precious Oben**
Data Analyst and Engineer
[preciousoben.com](https://preciousoben.com) · [LinkedIn](https://linkedin.com/in/preciousoben)
