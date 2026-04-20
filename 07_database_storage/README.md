# 07 Database Storage

This module contains a custom storage engine implemented in pure Python.

## What It Does

The engine in [storage_engine.py](storage_engine.py) is a small persistent key-value store with:

- append-only segment files
- an in-memory primary index for fast lookups
- secondary indexes on top-level JSON fields
- recovery by replaying segment files on startup
- tombstones for deletes
- compaction to remove stale records
- a small CLI for inserts, reads, scans, and indexed queries

## Why This Design

A plain JSON dump would be simpler, but it would hide the interesting storage-engine ideas.
This design keeps the implementation approachable while still showing how real systems think about:

- write amplification
- immutable logs
- crash recovery
- garbage collection through compaction
- query acceleration through secondary indexes

## Run the Demo

```bash
python3 07_database_storage/storage_engine.py
```

The demo creates a temporary store under `07_database_storage/data/demo_store`, writes records, restarts the engine, and then compacts old data.

## CLI Examples

Create or reuse a store and declare indexed fields:

```bash
python3 07_database_storage/storage_engine.py \
  --data-dir 07_database_storage/data/shop \
  --indexed-fields tier,city \
  set user:1 '{"name":"Aarav","tier":"gold","city":"Mumbai"}'
```

Read a key:

```bash
python3 07_database_storage/storage_engine.py \
  --data-dir 07_database_storage/data/shop \
  get user:1
```

Range scan by key:

```bash
python3 07_database_storage/storage_engine.py \
  --data-dir 07_database_storage/data/shop \
  range-scan --start user:1 --end user:9
```

Query via a secondary index:

```bash
python3 07_database_storage/storage_engine.py \
  --data-dir 07_database_storage/data/shop \
  query tier gold
```
