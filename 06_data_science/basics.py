import numpy as np
import pandas as pd


"""
loc: label-based indexing — uses row/column names. 
iloc: integer position-based — uses 0-based position. 
at: fast scalar access by label (single cell). iat: fast scalar by position.
"""


def example_loc():
    df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': ['x', 'y', 'z']
    })
    print(df.loc[0])  # Access the first row by label
    # print(df.loc[0, 'A'])  # Access a specific value by row label and column name
    # print(df.loc[0:1])  # Access a range of rows by label
    # print(df.loc[df['A'] > 1])  # Access rows based on a condition


def example_iloc():
    df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': ['x', 'y', 'z']
    })
    print(df.iloc[0])  # Access the first row by integer position
    # print(df.iloc[0, 0])  # Access a specific value by row and column integer position
    # print(df.iloc[0:2])  # Access a range of rows by integer position
    # print(df.iloc[df['A'] > 1])  # Access rows based on a condition (note: iloc doesn't support boolean indexing directly)


def example_at():
    df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': ['x', 'y', 'z']
    })
    print(df.at[0, 'A'])  # Access a single value by row label and column name


def example_iat():
    df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': ['x', 'y', 'z']
    })
    # Access a single value by row and column integer position
    print(df.iat[0, 0])


"""
GroupBy splits the DataFrame into groups, applies a function, then combines results. 

agg reduces each group to one row (e.g. sum per client). 

transform returns a Series with the same index as the original — each row gets its group's result.
"""


def example_groupby():
    df = pd.DataFrame({
        'Category': ['A', 'A', 'B', 'B'],
        'Value': [10, 20, 30, 40]
    })
    grouped = df.groupby('Category')['Value'].sum()
    print(grouped)


def example_transform():
    df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': ['x', 'y', 'z']
    })
    transformed = df['A'].transform(lambda x: x * 2)
    print(transformed)


def example_apply():
    df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': ['x', 'y', 'z']
    })
    applied = df.apply(lambda x: x.sum() if x.dtype ==
                       'int64' else x.str.upper())
    print(applied)


"""
Causes `SettingWithCopyWarning` and how do you fix it?
"""


def setting_with_copy_warning():
    df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': ['x', 'y', 'z']
    })
    slice_df = df[df['A'] > 1]  # This creates a view (not a copy)
    # Modifying the view raises a warning
    slice_df['B'] = slice_df['B'].str.upper()
    print(slice_df)


"""
handle a 10GB CSV without running out of memory?
"""


def handle_large_csv():
    import os
    chunk_size = 100_000  # Adjust based on available memory
    for chunk in pd.read_csv('large_file.csv', chunksize=chunk_size):
        # Process each chunk (e.g., filter, aggregate)
        processed_chunk = chunk[chunk['value'] > 100]  # Example processing
        # Append or save results as needed
        processed_chunk.to_csv('processed_large_file.csv', mode='a', header=not os.path.exists(
            'processed_large_file.csv'), index=False)


"""
Pivot Tables: pivot_table() aggregates data based on specified index/columns.
"""


def example_pivot_table():
    df = pd.DataFrame({
        'Category': ['A', 'A', 'B', 'B'],
        'Subcategory': ['X', 'Y', 'X', 'Y'],
        'Value': [10, 20, 30, 40]
    })
    pivot = df.pivot_table(values='Value', index='Category',
                           columns='Subcategory', aggfunc='sum')
    print(pivot)


"""
 How would you compute a portfolio's maximum drawdown using Pandas?
"""


def compute_max_drawdown():
    df = pd.DataFrame({
        'Date': pd.date_range(start='2024-01-01', periods=5, freq='D'),
        'Portfolio Value': [100, 120, 110, 130, 90]
    })
    df['Cumulative Max'] = df['Portfolio Value'].cummax()
    df['Drawdown'] = (df['Portfolio Value'] -
                      df['Cumulative Max']) / df['Cumulative Max']
    max_drawdown = df['Drawdown'].min()
    print(f"Maximum Drawdown: {max_drawdown:.2%}")


def run_basics_practice():
    data = {
        "id": [1, 2, 3, 4, 5, 6],
        "name": ["A", "B", "C", "D", "E", "F"],
        "age": [23, 45, 31, 22, 35, 28],
        "city": ["Delhi", "Mumbai", "Delhi", "Pune", "Mumbai", "Delhi"],
        "salary": [50000, 80000, 60000, 45000, 70000, 52000],
        "date": pd.date_range(start="2024-01-01", periods=6, freq="D")
    }

    df = pd.DataFrame(data)
    df.to_csv("sample.csv", index=False)
    print(df)
    print(df.head())
    print(df.tail())
    print(df.shape)
    print(df.columns)

    print(df['name'])
    print(df[['name', 'age']])
    # filtering
    df_fil = df[df['age']> 24]


def practice():
    # LOAD
    orders = pd.read_csv("orders.csv", parse_dates=["order_date"])
    customers = pd.read_csv("customers.csv", parse_dates=["signup_date"])
    payments = pd.read_parquet("payments.parquet")

    # CLEAN
    orders = orders[orders["amount"] > 0].drop_duplicates()
    customers["segment"] = customers["segment"].fillna("Unknown")

    # FILTER
    recent_orders = orders.loc[
        orders["order_date"] >= "2023-01-01"
    ]

    # MERGE
    df = pd.merge(recent_orders, customers, on="customer_id")
    df = pd.merge(df, payments, on="order_id")

    df = df[df["payment_status"] == "Success"]

    # FEATURE ENGINEERING
    df["year"] = df["order_date"].dt.year
    df["month"] = df["order_date"].dt.month

    # GROUPBY
    customer_metrics = df.groupby("customer_id").agg({
        "order_id": "count",
        "amount": ["sum", "mean"],
        "order_date": "max"
    })

    # WINDOW FUNCTION
    df["cumulative_spend"] = df.groupby("customer_id")["amount"].cumsum()

    # EXPORT
    customer_metrics.to_parquet("final_customer_metrics.parquet")


if __name__ == "__main__":
    # example_groupby()
    # example_transform()
    # setting_with_copy_warning()
    # example_pivot_table()
    run_basics_practice()


"""
Function() OVER (
    partition by column
    order bu column
)

select emplployee_id, emp_name,
ROW_NUMBER() OVER(
    partition by department
    order by salary desc
) as row_num
from sales

select emp, dep, salary
dense_rank() over(
    partition by dep
    order by salary desc
) as dense_rank
from sales ;
"""
