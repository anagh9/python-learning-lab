"""
Problem 3
---------
Two DataFrames: 10M client transactions and a 500k FX rates table.
Join them by currency and date without running out of memory.

Strategy:
1. Normalize just the join keys (`currency`, `trade_date`)
2. Pre-index the smaller FX table once
3. Slice the large transactions table into chunks
4. Join each chunk against the indexed FX lookup
5. Stream the output or only concatenate for small demos/tests
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterator

import pandas as pd

CHUNK_SIZE = 250_000


@dataclass
class JoinStats:
    rows_processed: int = 0
    rows_missing_fx: int = 0
    chunks_processed: int = 0


def _parse_dates(series: pd.Series) -> pd.Series:
    """
    Parse mixed date formats and normalize timestamps to midnight.
    """
    try:
        return pd.to_datetime(series, format="mixed").dt.normalize()
    except (TypeError, ValueError):
        return pd.to_datetime(series).dt.normalize()


def _normalize_join_keys(
    df: pd.DataFrame,
    date_col: str,
    currency_col: str,
) -> pd.DataFrame:
    """
    Normalize join columns without touching the rest of the schema.
    """
    normalized = df.copy()
    normalized[date_col] = _parse_dates(normalized[date_col])

    if normalized[currency_col].dtype == "object":
        normalized[currency_col] = normalized[currency_col].astype("category")

    return normalized


def prepare_fx_lookup(
    fx_rates: pd.DataFrame,
    date_col: str = "trade_date",
    currency_col: str = "currency",
) -> pd.DataFrame:
    """
    Prepare the smaller FX table for fast repeated lookups.

    Raises:
        ValueError: if the FX table has duplicate keys for a currency/date pair.
    """
    fx = _normalize_join_keys(fx_rates, date_col=date_col, currency_col=currency_col)

    duplicate_mask = fx.duplicated([currency_col, date_col], keep=False)
    if duplicate_mask.any():
        duplicate_keys = fx.loc[duplicate_mask, [currency_col, date_col]].head(5)
        raise ValueError(
            "FX rates must contain a single row per (currency, trade_date). "
            f"Sample duplicates:\n{duplicate_keys.to_string(index=False)}"
        )

    return fx.set_index([currency_col, date_col]).sort_index()


def iter_transaction_chunks(
    transactions: pd.DataFrame,
    chunk_size: int = CHUNK_SIZE,
) -> Iterator[pd.DataFrame]:
    """
    Yield transaction slices so we never build one huge merged copy in memory.
    """
    for start in range(0, len(transactions), chunk_size):
        yield transactions.iloc[start:start + chunk_size]


def iter_joined_transactions(
    transactions: pd.DataFrame,
    fx_rates: pd.DataFrame,
    chunk_size: int = CHUNK_SIZE,
    date_col: str = "trade_date",
    currency_col: str = "currency",
) -> Iterator[pd.DataFrame]:
    """
    Yield joined transaction chunks.

    This is the memory-safe path for truly large transaction tables.
    """
    fx_lookup = prepare_fx_lookup(
        fx_rates,
        date_col=date_col,
        currency_col=currency_col,
    )
    yield from _iter_joined_transactions_from_lookup(
        transactions,
        fx_lookup,
        chunk_size=chunk_size,
        date_col=date_col,
        currency_col=currency_col,
    )


def _iter_joined_transactions_from_lookup(
    transactions: pd.DataFrame,
    fx_lookup: pd.DataFrame,
    chunk_size: int,
    date_col: str,
    currency_col: str,
) -> Iterator[pd.DataFrame]:
    """
    Internal helper so we only prepare the FX lookup once per workflow.
    """

    for chunk in iter_transaction_chunks(transactions, chunk_size=chunk_size):
        normalized_chunk = _normalize_join_keys(
            chunk,
            date_col=date_col,
            currency_col=currency_col,
        )
        joined = normalized_chunk.join(
            fx_lookup,
            on=[currency_col, date_col],
            how="left",
            rsuffix="_fx",
        )
        yield joined


def join_transactions_with_fx(
    transactions: pd.DataFrame,
    fx_rates: pd.DataFrame,
    chunk_size: int = CHUNK_SIZE,
    date_col: str = "trade_date",
    currency_col: str = "currency",
) -> pd.DataFrame:
    """
    Convenience helper for smaller datasets and unit tests.

    Note:
        This concatenates all chunks back together, so it is not the best option
        for the full 10M-row workload. Prefer `iter_joined_transactions()` or
        `write_joined_transactions()` for large inputs.
    """
    fx_lookup = prepare_fx_lookup(
        fx_rates,
        date_col=date_col,
        currency_col=currency_col,
    )
    joined_chunks = list(
        _iter_joined_transactions_from_lookup(
            transactions,
            fx_lookup,
            chunk_size=chunk_size,
            date_col=date_col,
            currency_col=currency_col,
        )
    )
    if not joined_chunks:
        return transactions.iloc[0:0].copy()
    return pd.concat(joined_chunks, ignore_index=True)


def write_joined_transactions(
    transactions: pd.DataFrame,
    fx_rates: pd.DataFrame,
    output_file: str | Path,
    chunk_size: int = CHUNK_SIZE,
    date_col: str = "trade_date",
    currency_col: str = "currency",
) -> JoinStats:
    """
    Stream the joined result to disk chunk by chunk.

    This is the recommended option when the output itself is also very large.
    """
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fx_lookup = prepare_fx_lookup(
        fx_rates,
        date_col=date_col,
        currency_col=currency_col,
    )
    fx_columns = list(fx_lookup.columns)

    stats = JoinStats()
    first_chunk = True

    for joined_chunk in _iter_joined_transactions_from_lookup(
        transactions,
        fx_lookup,
        chunk_size=chunk_size,
        date_col=date_col,
        currency_col=currency_col,
    ):
        joined_chunk.to_csv(
            output_path,
            mode="w" if first_chunk else "a",
            header=first_chunk,
            index=False,
        )

        stats.rows_processed += len(joined_chunk)
        stats.chunks_processed += 1

        if fx_columns:
            stats.rows_missing_fx += int(
                joined_chunk[fx_columns].isna().all(axis=1).sum()
            )

        first_chunk = False

    return stats


def build_demo_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Small sample data to show the approach working end to end.
    """
    transactions = pd.DataFrame(
        {
            "transaction_id": [101, 102, 103, 104, 105],
            "client_id": ["C001", "C002", "C003", "C004", "C005"],
            "trade_date": [
                "2024-01-01",
                "2024/01/01",
                "Jan 02 2024",
                "2024-01-03",
                "2024-01-03",
            ],
            "currency": ["USD", "EUR", "JPY", "EUR", "GBP"],
            "amount_local": [1000.0, 2000.0, 150000.0, 800.0, 600.0],
        }
    )

    fx_rates = pd.DataFrame(
        {
            "trade_date": [
                "2024-01-01",
                "2024-01-01",
                "2024-01-02",
                "2024-01-03",
            ],
            "currency": ["USD", "EUR", "JPY", "EUR"],
            "usd_rate": [1.00, 1.10, 0.0068, 1.11],
        }
    )

    return transactions, fx_rates


if __name__ == "__main__":
    transactions_df, fx_rates_df = build_demo_data()

    joined_df = join_transactions_with_fx(
        transactions_df,
        fx_rates_df,
        chunk_size=2,
    )

    print("Joined transactions:")
    print("-" * 70)
    print(joined_df.to_string(index=False))
    print("\nMissing FX rows:", int(joined_df["usd_rate"].isna().sum()))

    stats = write_joined_transactions(
        transactions_df,
        fx_rates_df,
        output_file="06_data_science/temp/joined_transactions_demo.csv",
        chunk_size=2,
    )

    print("\nStream-to-disk stats:")
    print(asdict(stats))
