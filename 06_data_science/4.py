"""
Sample ETL Pipeline with DAG
----------------------------
A small, runnable ETL example that models task dependencies as a DAG.

DAG shape:
    seed_sample_data
      |--> extract_customers --> transform_customers --|
      |                                                |--> build_sales_mart --> load_sales_mart
      |--> extract_orders -----> transform_orders -----|                          |
                                                                                  --> write_run_report
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
import json

import pandas as pd

TEMP_DIR = Path(__file__).resolve().parent / "temp"
RAW_CUSTOMERS_FILE = TEMP_DIR / "etl_customers_raw.csv"
RAW_ORDERS_FILE = TEMP_DIR / "etl_orders_raw.csv"
CURATED_SALES_FILE = TEMP_DIR / "etl_daily_sales.csv"
RUN_REPORT_FILE = TEMP_DIR / "etl_run_report.json"


@dataclass
class Task:
    name: str
    func: Callable[[dict[str, Any]], Any]
    depends_on: tuple[str, ...] = ()


class DAG:
    def __init__(self, name: str):
        self.name = name
        self.tasks: dict[str, Task] = {}

    def add_task(
        self,
        name: str,
        func: Callable[[dict[str, Any]], Any],
        depends_on: tuple[str, ...] = (),
    ) -> None:
        if name in self.tasks:
            raise ValueError(f"Task already exists: {name}")
        self.tasks[name] = Task(name=name, func=func, depends_on=depends_on)

    def topological_order(self) -> list[str]:
        missing_dependencies: list[tuple[str, str]] = []
        in_degree = {name: 0 for name in self.tasks}
        graph = {name: [] for name in self.tasks}

        for task in self.tasks.values():
            for dependency in task.depends_on:
                if dependency not in self.tasks:
                    missing_dependencies.append((task.name, dependency))
                    continue
                graph[dependency].append(task.name)
                in_degree[task.name] += 1

        if missing_dependencies:
            missing_text = ", ".join(
                f"{task} -> {dependency}" for task, dependency in missing_dependencies
            )
            raise ValueError(f"Unknown dependencies found: {missing_text}")

        queue = deque(sorted(name for name, degree in in_degree.items() if degree == 0))
        order: list[str] = []

        while queue:
            current = queue.popleft()
            order.append(current)

            for downstream in sorted(graph[current]):
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)

        if len(order) != len(self.tasks):
            raise ValueError("Cycle detected in DAG definition.")

        return order

    def run(self, initial_state: dict[str, Any] | None = None) -> dict[str, Any]:
        state = initial_state or {}
        state.setdefault("results", {})
        state.setdefault("metrics", {})
        state.setdefault("artifacts", {})

        execution_order = self.topological_order()
        state["task_order"] = execution_order

        for task_name in execution_order:
            task = self.tasks[task_name]
            state["results"][task_name] = task.func(state)

        return state


def parse_mixed_dates(series: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(series, format="mixed")
    except (TypeError, ValueError):
        return pd.to_datetime(series)


def seed_sample_data(state: dict[str, Any]) -> dict[str, str]:
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    customers = pd.DataFrame(
        [
            {"customer_id": "C001", "customer_name": "Aarav", "city": "mumbai", "segment": "enterprise"},
            {"customer_id": "C002", "customer_name": "Diya", "city": "Bengaluru", "segment": "small_business"},
            {"customer_id": "C003", "customer_name": "Kabir", "city": "mumbai", "segment": None},
            {"customer_id": "C004", "customer_name": "Meera", "city": None, "segment": "consumer"},
        ]
    )

    orders = pd.DataFrame(
        [
            {"order_id": 1001, "customer_id": "C001", "order_date": "2024-04-01", "quantity": 2, "unit_price": 1200.0, "status": "completed"},
            {"order_id": 1002, "customer_id": "C002", "order_date": "2024/04/01", "quantity": 1, "unit_price": 800.0, "status": "completed"},
            {"order_id": 1003, "customer_id": "C003", "order_date": "Apr 2 2024", "quantity": 3, "unit_price": 500.0, "status": "completed"},
            {"order_id": 1004, "customer_id": "C003", "order_date": "2024-04-02", "quantity": 1, "unit_price": 650.0, "status": "cancelled"},
            {"order_id": 1005, "customer_id": "C004", "order_date": "2024-04-03", "quantity": 4, "unit_price": 300.0, "status": "completed"},
            {"order_id": 1006, "customer_id": "C999", "order_date": "2024-04-03", "quantity": 2, "unit_price": 450.0, "status": "completed"},
        ]
    )

    customers.to_csv(RAW_CUSTOMERS_FILE, index=False)
    orders.to_csv(RAW_ORDERS_FILE, index=False)

    state["artifacts"]["raw_customers"] = str(RAW_CUSTOMERS_FILE)
    state["artifacts"]["raw_orders"] = str(RAW_ORDERS_FILE)

    return {
        "customers_file": str(RAW_CUSTOMERS_FILE),
        "orders_file": str(RAW_ORDERS_FILE),
    }


def extract_customers(state: dict[str, Any]) -> pd.DataFrame:
    return pd.read_csv(RAW_CUSTOMERS_FILE)


def extract_orders(state: dict[str, Any]) -> pd.DataFrame:
    return pd.read_csv(RAW_ORDERS_FILE)


def transform_customers(state: dict[str, Any]) -> pd.DataFrame:
    customers = state["results"]["extract_customers"].copy()
    customers["city"] = customers["city"].fillna("Unknown").str.title()
    customers["segment"] = customers["segment"].fillna("consumer").str.replace("_", " ")
    customers = customers.drop_duplicates(subset=["customer_id"])
    return customers


def transform_orders(state: dict[str, Any]) -> pd.DataFrame:
    orders = state["results"]["extract_orders"].copy()
    orders["order_date"] = parse_mixed_dates(orders["order_date"])
    orders["status"] = orders["status"].str.lower()
    orders = orders[orders["status"] == "completed"].copy()
    orders = orders[orders["quantity"] > 0].copy()
    orders["gross_revenue"] = orders["quantity"] * orders["unit_price"]
    orders["order_date"] = orders["order_date"].dt.normalize()
    return orders


def build_sales_mart(state: dict[str, Any]) -> pd.DataFrame:
    customers = state["results"]["transform_customers"]
    orders = state["results"]["transform_orders"]

    enriched = orders.merge(customers, on="customer_id", how="left")
    state["metrics"]["orders_without_customer"] = int(
        enriched["customer_name"].isna().sum()
    )

    enriched["customer_name"] = enriched["customer_name"].fillna("Unknown")
    enriched["city"] = enriched["city"].fillna("Unknown")
    enriched["segment"] = enriched["segment"].fillna("unknown")

    sales_mart = (
        enriched.groupby(["order_date", "city", "segment"], as_index=False)
        .agg(
            total_orders=("order_id", "nunique"),
            unique_customers=("customer_id", "nunique"),
            total_revenue=("gross_revenue", "sum"),
        )
        .sort_values(["order_date", "total_revenue"], ascending=[True, False])
    )

    sales_mart["total_revenue"] = sales_mart["total_revenue"].round(2)
    return sales_mart


def load_sales_mart(state: dict[str, Any]) -> dict[str, Any]:
    sales_mart = state["results"]["build_sales_mart"]
    sales_mart.to_csv(CURATED_SALES_FILE, index=False)
    state["artifacts"]["curated_sales"] = str(CURATED_SALES_FILE)
    return {"output_file": str(CURATED_SALES_FILE), "rows_written": int(len(sales_mart))}


def write_run_report(state: dict[str, Any]) -> dict[str, Any]:
    state["artifacts"]["run_report"] = str(RUN_REPORT_FILE)

    report = {
        "pipeline_name": "sample_etl_with_dag",
        "task_order": state["task_order"],
        "raw_customers_rows": int(len(state["results"]["extract_customers"])),
        "raw_orders_rows": int(len(state["results"]["extract_orders"])),
        "transformed_orders_rows": int(len(state["results"]["transform_orders"])),
        "sales_mart_rows": int(len(state["results"]["build_sales_mart"])),
        "orders_without_customer": state["metrics"]["orders_without_customer"],
        "artifacts": state["artifacts"],
    }

    with open(RUN_REPORT_FILE, "w", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2, default=str)

    return report


def build_pipeline() -> DAG:
    dag = DAG(name="sample_etl_with_dag")
    dag.add_task("seed_sample_data", seed_sample_data)
    dag.add_task("extract_customers", extract_customers, depends_on=("seed_sample_data",))
    dag.add_task("extract_orders", extract_orders, depends_on=("seed_sample_data",))
    dag.add_task(
        "transform_customers",
        transform_customers,
        depends_on=("extract_customers",),
    )
    dag.add_task(
        "transform_orders",
        transform_orders,
        depends_on=("extract_orders",),
    )
    dag.add_task(
        "build_sales_mart",
        build_sales_mart,
        depends_on=("transform_customers", "transform_orders"),
    )
    dag.add_task(
        "load_sales_mart",
        load_sales_mart,
        depends_on=("build_sales_mart",),
    )
    dag.add_task(
        "write_run_report",
        write_run_report,
        depends_on=("load_sales_mart",),
    )
    return dag


if __name__ == "__main__":
    pipeline = build_pipeline()
    run_state = pipeline.run()

    print("DAG execution order:")
    print(" -> ".join(run_state["task_order"]))
    print("\nCurated sales mart:")
    print(run_state["results"]["build_sales_mart"].to_string(index=False))
    print(f"\nCSV output:  {CURATED_SALES_FILE}")
    print(f"Run report:  {RUN_REPORT_FILE}")
