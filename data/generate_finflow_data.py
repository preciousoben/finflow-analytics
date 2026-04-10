"""
FinFlow Analytics — Synthetic Data Generator
=============================================
Generates 4 tables with ~15,000 records total for a fintech payments platform.
Intentional errors are inserted to simulate real-world data quality issues.

Tables:
  - customers.csv        (~2,000 rows)
  - subscriptions.csv    (~2,000 rows)
  - transactions.csv     (~10,000 rows)
  - monthly_revenue.csv  (~1,000 rows)

Run: python3 generate_finflow_data.py
Output: /data/raw/
"""

import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

random.seed(99)
np.random.seed(99)

os.makedirs("data/raw", exist_ok=True)

START_DATE   = datetime(2024, 1, 1)
END_DATE     = datetime(2024, 12, 31)
N_CUSTOMERS  = 2000

PLANS = {
    "starter":    {"mrr": 49,   "weight": 0.40},
    "growth":     {"mrr": 99,   "weight": 0.30},
    "pro":        {"mrr": 199,  "weight": 0.20},
    "enterprise": {"mrr": 499,  "weight": 0.10},
}

CHANNELS = {
    "organic_search": {"weight": 0.28, "cac": 55},
    "paid_ads":       {"weight": 0.24, "cac": 210},
    "referral":       {"weight": 0.22, "cac": 70},
    "social_media":   {"weight": 0.16, "cac": 110},
    "email_campaign": {"weight": 0.10, "cac": 80},
}

PAYMENT_METHODS = ["card", "bank_transfer", "wallet", "crypto"]
CURRENCIES      = ["USD", "USD", "USD", "EUR", "GBP", "USD", "USD"]
COUNTRIES       = ["USA","UK","Canada","Germany","France","Australia",
                   "Netherlands","Sweden","Nigeria","Kenya","South Africa",
                   "Cameroon","Ghana","India","Brazil"]

CHURN_RATES = {
    "starter": 0.07, "growth": 0.045,
    "pro": 0.03, "enterprise": 0.015,
}

TX_STATUSES = {
    "success": 0.87,
    "failed":  0.07,
    "pending": 0.04,
    "refunded":0.02,
}

def rdate(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def wchoice(d):
    return random.choices(list(d.keys()), weights=[v["weight"] for v in d.values()], k=1)[0]

def wchoice_simple(keys, weights):
    return random.choices(keys, weights=weights, k=1)[0]

def fmt_date_mixed(dt):
    """Randomly mix date formats to simulate real-world inconsistency"""
    fmt = random.choice([
        "%Y-%m-%d",   # 2024-03-15  (most common)
        "%Y-%m-%d",
        "%Y-%m-%d",
        "%d/%m/%Y",   # 15/03/2024
        "%m/%d/%Y",   # 03/15/2024  (US format)
        "%d-%m-%Y",   # 15-03-2024
    ])
    return dt.strftime(fmt)

# 1. CUSTOMERS 
print("Generating customers...")
customers = []
for i in range(1, N_CUSTOMERS + 1):
    channel     = wchoice(CHANNELS)
    signup_date = rdate(START_DATE, END_DATE - timedelta(days=30))
    cac_base    = CHANNELS[channel]["cac"]
    cac         = round(cac_base * np.random.uniform(0.65, 1.5), 2)
    country     = random.choice(COUNTRIES)

    row = {
        "customer_id":   f"CUST-{i:05d}",
        "signup_date":   fmt_date_mixed(signup_date),
        "full_name":     f"Customer {i}",
        "email":         f"user{i}@{'gmail.com' if random.random()<0.5 else 'company.io'}",
        "country":       country,
        "company_size":  random.choice(["1-10","11-50","51-200","201-500","500+"]),
        "acquisition_channel": channel,
        "cac_usd":       cac,
        "industry":      random.choice(["fintech","ecommerce","saas","healthcare",
                                        "logistics","media","education"]),
    }

    # Intentional errors
    # ~2% have NULL email
    if random.random() < 0.02:
        row["email"] = None

    # ~1.5% have negative CAC (data entry error)
    if random.random() < 0.015:
        row["cac_usd"] = -abs(cac)

    # ~1% duplicate customer IDs (will be two rows with same ID)
    if random.random() < 0.01 and i > 1:
        row["customer_id"] = f"CUST-{random.randint(1, i-1):05d}"

    customers.append(row)

df_customers = pd.DataFrame(customers)
df_customers.to_csv("data/raw/customers.csv", index=False)
print(f"  customers.csv: {len(df_customers):,} rows")


# 2. SUBSCRIPTIONS
print("Generating subscriptions...")
subscriptions = []
sub_id = 1
valid_customer_ids = [f"CUST-{i:05d}" for i in range(1, N_CUSTOMERS + 1)]

for _, cust in df_customers.drop_duplicates("customer_id").iterrows():
    plan       = wchoice(PLANS)
    mrr        = PLANS[plan]["mrr"]
    start_date = pd.to_datetime(cust["signup_date"], dayfirst=False, errors="coerce")
    if pd.isna(start_date):
        start_date = rdate(START_DATE, END_DATE - timedelta(days=30))

    churn_prob    = CHURN_RATES[plan]
    months_active = random.randint(1, 12)
    churned       = random.random() < churn_prob * months_active
    end_date      = None

    if churned:
        end_date = start_date + timedelta(days=30 * months_active)
        if end_date > END_DATE:
            end_date = None
            churned  = False

    # upgrade logic
    plan_keys    = list(PLANS.keys())
    is_upgraded  = False
    upgrade_plan = None
    upgrade_date = None
    upgrade_mrr  = None
    if not churned and plan != "enterprise" and random.random() < 0.14:
        is_upgraded  = True
        upgrade_plan = plan_keys[plan_keys.index(plan) + 1]
        upgrade_date = start_date + timedelta(days=random.randint(60, 200))
        upgrade_mrr  = PLANS[upgrade_plan]["mrr"]
        if upgrade_date > END_DATE:
            is_upgraded = False

    row = {
        "subscription_id": f"SUB-{sub_id:05d}",
        "customer_id":     cust["customer_id"],
        "plan":            plan,
        "mrr_usd":         mrr,
        "start_date":      fmt_date_mixed(start_date) if isinstance(start_date, datetime) else str(start_date)[:10],
        "end_date":        fmt_date_mixed(end_date) if end_date else None,
        "status":          "churned" if churned else "active",
        "is_upgraded":     is_upgraded,
        "upgrade_plan":    upgrade_plan,
        "upgrade_date":    fmt_date_mixed(upgrade_date) if upgrade_date else None,
        "upgrade_mrr_usd": upgrade_mrr,
        "billing_cycle":   random.choice(["monthly", "monthly", "annual"]),
    }

    # Intentional errors
    # ~2% have NULL plan
    if random.random() < 0.02:
        row["plan"] = None

    # ~1.5% have mrr = 0 (missed billing)
    if random.random() < 0.015:
        row["mrr_usd"] = 0

    # ~1% duplicate subscription for same customer
    if random.random() < 0.01:
        dup = row.copy()
        dup["subscription_id"] = f"SUB-{sub_id+5000:05d}"
        subscriptions.append(dup)

    subscriptions.append(row)
    sub_id += 1

df_subscriptions = pd.DataFrame(subscriptions)
df_subscriptions.to_csv("data/raw/subscriptions.csv", index=False)
print(f"  subscriptions.csv: {len(df_subscriptions):,} rows")


# 3. TRANSACTIONS
print("Generating transactions...")
transactions = []
tx_id = 1
all_customer_ids = df_customers["customer_id"].tolist()

# Generate ~10,000 transactions spread across the year
for _ in range(10000):
    cust_id    = random.choice(all_customer_ids)
    tx_date    = rdate(START_DATE, END_DATE)
    method     = random.choice(PAYMENT_METHODS)
    currency   = random.choice(CURRENCIES)
    status     = wchoice_simple(
        list(TX_STATUSES.keys()),
        list(TX_STATUSES.values())
    )

    # Amount varies by payment type
    if method == "crypto":
        amount = round(random.uniform(10, 5000), 2)
    elif method == "bank_transfer":
        amount = round(random.uniform(100, 10000), 2)
    else:
        amount = round(random.uniform(10, 2000), 2)

    # Failed transactions often have lower amounts
    if status == "failed":
        amount = round(random.uniform(5, 500), 2)

    row = {
        "transaction_id":  f"TXN-{tx_id:06d}",
        "customer_id":     cust_id,
        "transaction_date":fmt_date_mixed(tx_date),
        "amount":          amount,
        "currency":        currency,
        "payment_method":  method,
        "status":          status,
        "fee_usd":         round(amount * random.uniform(0.01, 0.03), 2),
        "channel":         random.choice(["api","dashboard","mobile","web"]),
        "country":         random.choice(COUNTRIES),
        "description":     random.choice([
            "subscription_payment","one_time_payment",
            "refund_processed","wallet_topup","transfer"
        ]),
    }

    # Intentional errors
    # ~3% have NULL customer_id
    if random.random() < 0.03:
        row["customer_id"] = None

    # ~2% have negative amounts
    if random.random() < 0.02:
        row["amount"] = -abs(amount)

    # ~1.5% have zero amount
    if random.random() < 0.015:
        row["amount"] = 0.0

    # ~2% duplicate transaction IDs (system glitch)
    if random.random() < 0.02 and tx_id > 100:
        row["transaction_id"] = f"TXN-{random.randint(1, tx_id-1):06d}"

    # ~1% have NULL status
    if random.random() < 0.01:
        row["status"] = None

    # ~0.5% have future dates (data entry error)
    if random.random() < 0.005:
        future = datetime(2025, random.randint(1, 6), random.randint(1, 28))
        row["transaction_date"] = fmt_date_mixed(future)

    transactions.append(row)
    tx_id += 1

df_transactions = pd.DataFrame(transactions)
df_transactions.to_csv("data/raw/transactions.csv", index=False)
print(f"  transactions.csv: {len(df_transactions):,} rows")


# 4. MONTHLY REVENUE
print("Generating monthly revenue...")
monthly_rows = []

clean_subs = df_subscriptions.drop_duplicates("subscription_id").copy()
clean_subs["start_date_parsed"] = pd.to_datetime(
    clean_subs["start_date"], dayfirst=False, errors="coerce"
)

for _, sub in clean_subs.iterrows():
    start = sub["start_date_parsed"]
    if pd.isna(start):
        continue

    end_raw = sub["end_date"]
    end = pd.to_datetime(end_raw, dayfirst=False, errors="coerce") if end_raw else pd.to_datetime(END_DATE)
    if pd.isna(end):
        end = pd.to_datetime(END_DATE)

    mrr = sub["mrr_usd"] if sub["mrr_usd"] and sub["mrr_usd"] > 0 else PLANS.get(sub["plan"] or "starter", {"mrr": 49})["mrr"]

    current = start.replace(day=1)
    while current <= end and current <= pd.to_datetime(END_DATE):
        # apply upgrade if applicable
        if sub["is_upgraded"] and sub["upgrade_date"]:
            ug_month = pd.to_datetime(sub["upgrade_date"], dayfirst=False, errors="coerce")
            if not pd.isna(ug_month):
                ug_month = ug_month.replace(day=1)
                if current >= ug_month and sub["upgrade_mrr_usd"]:
                    mrr = sub["upgrade_mrr_usd"]

        monthly_rows.append({
            "revenue_id":      f"REV-{len(monthly_rows)+1:06d}",
            "subscription_id": sub["subscription_id"],
            "customer_id":     sub["customer_id"],
            "month":           current.strftime("%Y-%m-%d"),
            "mrr_usd":         mrr,
            "plan":            sub["plan"],
            "billing_cycle":   sub["billing_cycle"],
        })

        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

df_revenue = pd.DataFrame(monthly_rows)
df_revenue.to_csv("data/raw/monthly_revenue.csv", index=False)
print(f"  monthly_revenue.csv: {len(df_revenue):,} rows")


# SUMMARY
total = len(df_customers) + len(df_subscriptions) + len(df_transactions) + len(df_revenue)
print(f"\n{'='*50}")
print(f"FINFLOW DATA GENERATION COMPLETE")
print(f"{'='*50}")
print(f"Total records:       {total:,}")
print(f"  customers:         {len(df_customers):,}")
print(f"  subscriptions:     {len(df_subscriptions):,}")
print(f"  transactions:      {len(df_transactions):,}")
print(f"  monthly_revenue:   {len(df_revenue):,}")
print(f"\nData quality issues seeded:")
print(f"  Duplicate customer IDs:     ~{int(N_CUSTOMERS*0.01)} rows")
print(f"  NULL emails:                ~{int(N_CUSTOMERS*0.02)} rows")
print(f"  Negative CAC values:        ~{int(N_CUSTOMERS*0.015)} rows")
print(f"  NULL plan in subscriptions: ~{int(N_CUSTOMERS*0.02)} rows")
print(f"  Zero MRR rows:              ~{int(N_CUSTOMERS*0.015)} rows")
print(f"  Duplicate subscription IDs: ~{int(N_CUSTOMERS*0.01)} rows")
print(f"  NULL customer_id (txns):    ~{int(10000*0.03)} rows")
print(f"  Negative amounts (txns):    ~{int(10000*0.02)} rows")
print(f"  Duplicate transaction IDs:  ~{int(10000*0.02)} rows")
print(f"  Mixed date formats:         throughout all tables")
print(f"  Future-dated transactions:  ~{int(10000*0.005)} rows")
print(f"\nOutput: data/raw/")
