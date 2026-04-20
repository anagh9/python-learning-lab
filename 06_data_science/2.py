"""
India Equity PnL Pipeline
=========================
Processes a 15M-row trade CSV efficiently using:
  - Chunked reading       → constant memory regardless of file size
  - Early column pruning  → only load columns we need
  - Vectorized PnL calc   → no iterrows, no apply(axis=1)
  - Dtype optimization    → category + float32 cuts RAM ~60%
  - Parallel chunk fan-out → ThreadPoolExecutor for I/O overlap
  - Accumulator pattern   → never concat growing DataFrames

Output: top_10_pnl_per_day.csv   — top 10 traders by PnL for each date
        pipeline_run_stats.json  — run metadata & timing
"""

import pandas as pd
import numpy as np
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Iterator

# ── Logging — structured JSON ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","lvl":"%(levelname)s","msg":"%(message)s"}',
    datefmt="%H:%M:%S",
)
log = logging.getLogger("india_equity_pnl")

# ── Config ────────────────────────────────────────────────────────────────────
CHUNK_SIZE = 100_000     # rows per chunk — ~20MB each in memory
MAX_WORKERS = 8           # parallel processing threads
STATS_FILE = "temp/pipeline_run_stats.json"
TOP_N = 10          # top N traders per day

# Only load columns we actually need — massive memory saving on 15M rows
USECOLS = [
    "trader_id", "trade_date", "country",
    "asset_class", "buy_sell", "quantity",
    "entry_price", "exit_price", "status",
]

# Dtype map — applied at read time, before any data hits RAM
# category for low-cardinality strings = huge memory win
DTYPES = {
    "trader_id":   "category",
    "country":     "category",
    "asset_class": "category",
    "buy_sell":    "category",
    "status":      "category",
    "quantity":    "int32",
    "entry_price": "float32",
    "exit_price":  "float32",
}

# ── Run metrics dataclass ─────────────────────────────────────────────────────


@dataclass
class RunStats:
    start_time:      float = field(default_factory=time.perf_counter)
    total_rows_read: int = 0
    rows_filtered:   int = 0    # India + Equity + COMPLETED
    rows_rejected:   int = 0    # everything else
    chunks_done:     int = 0
    errors:          int = 0
    elapsed_sec:     float = 0.0
    throughput_kps:  float = 0.0  # thousands of rows per second

    def finalize(self):
        self.elapsed_sec = round(time.perf_counter() - self.start_time, 2)
        self.throughput_kps = round(
            self.total_rows_read / self.elapsed_sec / 1000, 1)


# ── Step 1: Chunk reader ──────────────────────────────────────────────────────
def read_chunks(filepath: str) -> Iterator[tuple[int, pd.DataFrame]]:
    """
    Yields (chunk_id, DataFrame) pairs from a CSV.
    Only loads USECOLS — ignore everything else at the OS level.
    Uses dtype map so Pandas never allocates object arrays for categoricals.
    """
    for chunk_id, chunk in enumerate(
        pd.read_csv(
            filepath,
            usecols=USECOLS,
            dtype=DTYPES,
            parse_dates=["trade_date"],
            chunksize=CHUNK_SIZE,
            on_bad_lines="warn",      # log malformed rows, don't crash
        )
    ):
        yield chunk_id, chunk


# ── Step 2: Filter ───────────────────────────────────────────────────────────
def filter_india_equity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only:
      - country == 'India'
      - asset_class == 'Equity'
      - status == 'COMPLETED'  (exclude pending/cancelled — not realised PnL)

    Uses boolean mask — single vectorized pass over the column arrays.
    No Python loop, no apply, no iterrows.
    """
    mask = (
        (df["country"] == "India") &
        (df["asset_class"] == "Equity") &
        (df["status"] == "COMPLETED")
    )
    return df.loc[mask].copy()


# ── Step 3: Compute PnL ──────────────────────────────────────────────────────
def compute_pnl(df: pd.DataFrame) -> pd.DataFrame:
    """
    PnL formula:
      BUY  trade: you bought at entry_price, sold at exit_price
                  PnL = (exit_price - entry_price) * quantity
      SELL trade: you sold short at entry_price, covered at exit_price
                  PnL = (entry_price - exit_price) * quantity

    Entire computation is vectorised — no row-by-row logic.
    np.where runs in C — ~100x faster than apply(axis=1).
    """
    price_diff = np.where(
        df["buy_sell"] == "BUY",
        df["exit_price"] - df["entry_price"],   # long  trade
        df["entry_price"] - df["exit_price"],     # short trade
    )
    # Keep as float64 for PnL (money — don't use float32 for financials)
    df["pnl"] = price_diff.astype("float64") * df["quantity"].astype("float64")
    return df[["trader_id", "trade_date", "pnl"]]


# ── Step 4: Process one chunk ─────────────────────────────────────────────────
def process_chunk(chunk_id: int, chunk: pd.DataFrame) -> pd.DataFrame | None:
    """
    Full pipeline for a single chunk.
    Returns a small aggregated DataFrame (one row per trader per date).
    This is what gets accumulated — never the raw filtered rows.
    """
    try:
        filtered = filter_india_equity(chunk)
        if filtered.empty:
            return None

        with_pnl = compute_pnl(filtered)

        # Aggregate to (trader_id, trade_date) → sum PnL for this chunk
        # This collapses 100k rows → typically a few hundred rows
        agg = (
            with_pnl
            .groupby(["trade_date", "trader_id"], observed=True)["pnl"]
            .sum()
            .reset_index()
        )
        return agg

    except Exception as exc:
        log.error(f"Chunk {chunk_id} failed: {exc}")
        return None


# ── Step 5: Top-N per day ─────────────────────────────────────────────────────
def top_n_per_day(df: pd.DataFrame, n: int = TOP_N) -> pd.DataFrame:
    """
    From the combined aggregated DataFrame (all chunks merged),
    return top N traders by total PnL for each trading day.

    Pattern: sort descending → groupby.head(n) — cleaner than apply+nlargest
    and avoids the Pandas apply() key-drop issue on grouped DataFrames.
    """
    top = (
        df.sort_values(["trade_date", "pnl"], ascending=[True, False])
        .groupby("trade_date", group_keys=False)
        .head(n)
        .reset_index(drop=True)
    )
    # Add rank within each day
    top["rank"] = (
        top.sort_values(["trade_date", "pnl"], ascending=[True, False])
           .groupby("trade_date")
           .cumcount() + 1
    )
    top = top.sort_values(["trade_date", "rank"])
    top["pnl"] = top["pnl"].round(2)
    return top[["trade_date", "rank", "trader_id", "pnl"]]


# ── Main orchestrator ─────────────────────────────────────────────────────────
def run_pipeline(filepath: str) -> pd.DataFrame:
    stats = RunStats()
    log.info(
        f"Pipeline started | file={filepath} | chunk_size={CHUNK_SIZE:,} | workers={MAX_WORKERS}")

    # Accumulate aggregated (not raw) results — stays tiny in memory
    # Key: (trade_date, trader_id) → total PnL
    # Using a dict avoids ever growing a list of DataFrames
    accumulator: dict[tuple, float] = {}

    chunk_gen = read_chunks(filepath)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        # Submit all chunks to thread pool
        futures = {
            pool.submit(process_chunk, cid, chunk): (cid, len(chunk))
            for cid, chunk in chunk_gen
        }

        for future in as_completed(futures):
            cid, chunk_len = futures[future]
            stats.total_rows_read += chunk_len
            stats.chunks_done += 1

            result = future.result()
            if result is None:
                stats.rows_rejected += chunk_len
                continue

            # Merge this chunk's aggregation into the accumulator
            # iterrows is fine here — result has << 1000 rows (already aggregated)
            for _, row in result.iterrows():
                key = (row["trade_date"], row["trader_id"])
                accumulator[key] = accumulator.get(key, 0.0) + row["pnl"]

            stats.rows_filtered += len(result)

            if stats.chunks_done % 20 == 0:
                elapsed = time.perf_counter() - stats.start_time
                tput = stats.total_rows_read / elapsed / 1_000
                log.info(
                    f"Progress: {stats.chunks_done} chunks | "
                    f"{stats.total_rows_read:,} rows | "
                    f"{tput:.0f}k rows/sec"
                )

    log.info(
        f"All chunks done — building final result from {len(accumulator):,} (date, trader) pairs")

    # Build final DataFrame from accumulator
    if not accumulator:
        log.warning("No India equity trades found in file.")
        return pd.DataFrame(columns=["trade_date", "rank", "trader_id", "pnl"])

    rows = [{"trade_date": d, "trader_id": t, "pnl": pnl}
            for (d, t), pnl in accumulator.items()]
    combined = pd.DataFrame(rows)
    combined["trade_date"] = pd.to_datetime(combined["trade_date"])

    result = top_n_per_day(combined, n=TOP_N)

    stats.finalize()
    stats.rows_rejected = stats.total_rows_read - stats.rows_filtered

    log.info(
        f"Pipeline complete | "
        f"rows_read={stats.total_rows_read:,} | "
        f"india_equity={stats.rows_filtered:,} | "
        f"rejected={stats.rows_rejected:,} | "
        f"elapsed={stats.elapsed_sec}s | "
        f"throughput={stats.throughput_kps}k rows/s"
    )

    # Save stats
    with open(STATS_FILE, "w") as f:
        json.dump(asdict(stats), f, indent=2, default=str)

    return result


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # INPUT_FILE = "temp/pnl_data.csv"
    INPUT_FILE = "temp/pnl_data_large.csv"
    OUTPUT_FILE = "temp/top_10_pnl_per_day.csv"
    
    output = run_pipeline(INPUT_FILE)

    if not output.empty:
        output.to_csv(OUTPUT_FILE, index=False)
        log.info(f"Results saved → {OUTPUT_FILE}")

        print("\n" + "="*60)
        print(f"TOP {TOP_N} TRADERS BY DAILY PnL — INDIA EQUITY")
        print("="*60)
        print(
            f"\nTotal trading days in output: {output['trade_date'].nunique()}")
        print(f"Total rows in output:         {len(output)}")
        print("\nSample — first 3 days:\n")

        sample_days = sorted(output["trade_date"].unique())[:3]
        sample = output[output["trade_date"].isin(sample_days)]
        print(sample.to_string(index=False))
    else:
        print("No results — check filters and input file.")
