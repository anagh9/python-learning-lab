import pandas as pd
import numpy as np

np.random.seed(42)

# =====================================================
# 1. CREATE DATAFRAMES
# =====================================================
customers = pd.DataFrame({
    "customer_id": range(1, 11),
    "name": ["Amit", "Sara", "John", "Priya", "Raj", "Neha", "Ali", "Zara", "Vikram", "Anya"],
    "signup_date": pd.date_range(start="2022-01-01", periods=10, freq="90D"),
    "segment": ["Premium", "Standard", None, "Premium", "Standard", None, "Premium", "Standard", "Premium", None]
})

orders = pd.DataFrame({
    "order_id": range(1001, 1031),
    "customer_id": np.random.choice(customers["customer_id"], 30),
    "order_date": pd.date_range(start="2023-01-01", periods=30, freq="15D"),
    "amount": np.random.randint(-200, 5000, 30),
    "city": np.random.choice(["Delhi", "Mumbai", "Pune", "Bangalore"], 30)
})

payments = pd.DataFrame({
    "order_id": orders["order_id"],
    "payment_method": np.random.choice(["UPI", "Card", "NetBanking"], 30),
    "payment_status": np.random.choice(["Success", "Failed"], 30, p=[0.8, 0.2])
})

print("Initial Data Created\n")


# =====================================================
# 2. SAVE TO FILES
# =====================================================
customers.to_csv("customers.csv", index=False)
orders.to_csv("orders.csv", index=False)
payments.to_parquet("payments.parquet", index=False)

print("Files Saved\n")


# =====================================================
# 3. LOAD FILES (READ)
# =====================================================
customers = pd.read_csv("customers.csv", parse_dates=["signup_date"])
orders = pd.read_csv("orders.csv", parse_dates=["order_date"])
payments = pd.read_parquet("payments.parquet")

print("Files Loaded\n")


# =====================================================
# 4. CLEANING
# =====================================================
orders = orders.loc[orders["amount"] > 0]   # loc filtering
orders = orders.drop_duplicates()

customers["segment"] = customers["segment"].fillna("Unknown")

print("Cleaning Done\n")


# =====================================================
# 5. FILTERING (LAST 2 YEARS)
# =====================================================
recent_orders = orders.loc[
    orders["order_date"] >= "2023-01-01"
]

# =====================================================
# 6. MERGE
# =====================================================
df = pd.merge(recent_orders, customers, on="customer_id")
df = pd.merge(df, payments, on="order_id")

# keep only successful payments
df = df.loc[df["payment_status"] == "Success"]

print("Merging Done\n")


# =====================================================
# 7. FEATURE ENGINEERING
# =====================================================
df["year"] = df["order_date"].dt.year
df["month"] = df["order_date"].dt.month

# categorize order value
conditions = [
    df["amount"] < 500,
    df["amount"].between(500, 2000),
    df["amount"] > 2000
]

choices = ["Low", "Medium", "High"]

df["order_category"] = np.select(conditions, choices)

print("Feature Engineering Done\n")


# =====================================================
# 8. WINDOW FUNCTIONS
# =====================================================
df = df.sort_values(["customer_id", "order_date"])

# cumulative spend
df["cumulative_spend"] = df.groupby("customer_id")["amount"].cumsum()

# rolling average (3 orders)
df["rolling_avg"] = (
    df.groupby("customer_id")["amount"]
      .rolling(3)
      .mean()
      .reset_index(level=0, drop=True)
)

# rank by amount
df["rank"] = df.groupby("customer_id")["amount"].rank(ascending=False)

print("Window Functions Done\n")


# =====================================================
# 9. GROUPBY AGGREGATION
# =====================================================
customer_metrics = df.groupby("customer_id").agg(
    total_orders=("order_id", "count"),
    total_spent=("amount", "sum"),
    avg_order_value=("amount", "mean"),
    last_order_date=("order_date", "max")
).reset_index()

print("Aggregation Done\n")


# =====================================================
# 10. ADVANCED FILTERING
# =====================================================
high_value_customers = customer_metrics.loc[
    (customer_metrics["total_orders"] >= 5) &
    (customer_metrics["total_spent"] > 10000)
]

print("High Value Customers:\n", high_value_customers, "\n")


# =====================================================
# 11. PIVOT TABLE
# =====================================================
pivot = df.pivot_table(
    values="amount",
    index="city",
    columns="month",
    aggfunc="sum"
)

print("Pivot Table:\n", pivot, "\n")


# =====================================================
# 12. SORT + INDEX
# =====================================================
customer_metrics = customer_metrics.sort_values("total_spent", ascending=False)
customer_metrics = customer_metrics.set_index("customer_id")

print("Sorted Metrics:\n", customer_metrics.head(), "\n")


# =====================================================
# 13. EXPORT FINAL OUTPUT
# =====================================================
customer_metrics.to_parquet("final_customer_metrics.parquet")

print("Final Output Saved\n")


# =====================================================
# 14. BONUS TASKS
# =====================================================

# Top 3 customers per city
df["city_rank"] = df.groupby("city")["amount"].rank(
    method="dense", ascending=False)
top_customers = df[df["city_rank"] <= 3]

print("Top Customers per City:\n",
      top_customers[["city", "customer_id", "amount"]])


# Inactive customers (no orders in last 6 months)
latest_date = df["order_date"].max()
inactive = customer_metrics[
    (latest_date - customer_metrics["last_order_date"]).dt.days > 180
]

print("\nInactive Customers:\n", inactive)
