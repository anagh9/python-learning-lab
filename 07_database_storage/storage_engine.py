"""
Custom Data Storage Engine
--------------------------
Educational key-value storage engine with:
  - Append-only segment files
  - In-memory primary index
  - Secondary indexes on top-level JSON fields
  - Crash recovery by replaying segments
  - Tombstones for deletes
  - Manual compaction
  - A tiny CLI/query layer

This is not a full database server. It is a compact storage-engine example
that shows how persistence, indexing, and compaction fit together.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import struct
import sys
from typing import Any, Iterator
import zlib


MAGIC = b"KVDB"
FLAG_SET = 1
FLAG_DELETE = 2
HEADER = struct.Struct(">4sBIII")
SEGMENT_TEMPLATE = "segment-{segment_id:06d}.db"
METADATA_FILE = "metadata.json"


class StorageEngineError(Exception):
    """Base error for the storage engine."""


class CorruptionError(StorageEngineError):
    """Raised when a segment contains invalid or corrupted records."""


class KeyNotFoundError(StorageEngineError):
    """Raised when a key does not exist and no default value is supplied."""


@dataclass(frozen=True)
class RecordPointer:
    segment_id: int
    offset: int


_MISSING = object()


class StorageEngine:
    def __init__(
        self,
        data_dir: str | Path,
        segment_size_limit: int = 1024 * 1024,
        sync_on_write: bool = False,
        indexed_fields: tuple[str, ...] | list[str] | None = None,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.segment_size_limit = segment_size_limit
        self.sync_on_write = sync_on_write
        self.indexed_fields = self._resolve_indexed_fields(indexed_fields)

        self._index: dict[str, RecordPointer] = {}
        self._secondary_indexes: dict[str, dict[str, set[str]]] = {
            field: {} for field in self.indexed_fields
        }
        self._indexed_values_by_key: dict[str, dict[str, str]] = {}
        self._active_segment_id = 1
        self._active_handle = None

        self._save_metadata()
        self._recover()

    def close(self) -> None:
        if self._active_handle is not None and not self._active_handle.closed:
            self._active_handle.close()

    def __enter__(self) -> "StorageEngine":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def set(self, key: str, value: Any) -> None:
        self._validate_key(key)
        self._remove_secondary_entries(key)
        record_offset = self._append_record(key, value, flag=FLAG_SET)
        self._index[key] = RecordPointer(self._active_segment_id, record_offset)
        self._add_secondary_entries(key, value)

    def get(self, key: str, default: Any = _MISSING) -> Any:
        pointer = self._index.get(key)
        if pointer is None:
            if default is _MISSING:
                raise KeyNotFoundError(f"Key not found: {key}")
            return default

        flag, stored_key, value = self._read_record(pointer.segment_id, pointer.offset)
        if flag == FLAG_DELETE or stored_key != key:
            if default is _MISSING:
                raise KeyNotFoundError(f"Key not found: {key}")
            return default
        return value

    def delete(self, key: str) -> bool:
        if key not in self._index:
            return False

        self._remove_secondary_entries(key)
        self._append_record(key, None, flag=FLAG_DELETE)
        self._index.pop(key, None)
        return True

    def exists(self, key: str) -> bool:
        return key in self._index

    def scan(self, prefix: str | None = None) -> Iterator[tuple[str, Any]]:
        for key in sorted(self._index):
            if prefix is not None and not key.startswith(prefix):
                continue
            yield key, self.get(key)

    def range_scan(
        self,
        start_key: str | None = None,
        end_key: str | None = None,
    ) -> Iterator[tuple[str, Any]]:
        for key in sorted(self._index):
            if start_key is not None and key < start_key:
                continue
            if end_key is not None and key > end_key:
                continue
            yield key, self.get(key)

    def query_by_field(self, field: str, value: Any) -> Iterator[tuple[str, Any]]:
        if field not in self._secondary_indexes:
            raise StorageEngineError(
                f"Field '{field}' is not indexed. Indexed fields: {list(self.indexed_fields)}"
            )

        token = self._encode_index_value(value)
        matching_keys = self._secondary_indexes[field].get(token, set())
        for key in sorted(matching_keys):
            yield key, self.get(key)

    def compact(self) -> None:
        temp_dir = self.data_dir / "_compaction_tmp"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)

        compacted_path = temp_dir / self._segment_filename(1)
        new_index: dict[str, RecordPointer] = {}
        new_index_tokens: dict[str, dict[str, str]] = {}
        new_secondary_indexes: dict[str, dict[str, set[str]]] = {
            field: {} for field in self.indexed_fields
        }

        with open(compacted_path, "wb") as compacted_handle:
            for key in sorted(self._index):
                value = self.get(key)
                offset = self._write_record(compacted_handle, key, value, FLAG_SET)
                new_index[key] = RecordPointer(1, offset)

                tokens = self._extract_index_tokens(value)
                if tokens:
                    new_index_tokens[key] = tokens
                    for field, token in tokens.items():
                        new_secondary_indexes[field].setdefault(token, set()).add(key)

        self.close()

        for segment_path in self._segment_paths():
            segment_path.unlink()

        final_path = self.data_dir / self._segment_filename(1)
        shutil.move(str(compacted_path), str(final_path))
        shutil.rmtree(temp_dir)

        self._index = new_index
        self._indexed_values_by_key = new_index_tokens
        self._secondary_indexes = new_secondary_indexes
        self._active_segment_id = 1
        self._active_handle = open(final_path, "ab+")
        self._active_handle.seek(0, os.SEEK_END)

    def stats(self) -> dict[str, Any]:
        segment_paths = self._segment_paths()
        return {
            "live_keys": len(self._index),
            "segments": len(segment_paths),
            "disk_size_bytes": sum(path.stat().st_size for path in segment_paths),
            "active_segment_id": self._active_segment_id,
            "indexed_fields": list(self.indexed_fields),
            "indexed_keys": len(self._indexed_values_by_key),
        }

    def _recover(self) -> None:
        segment_paths = self._segment_paths()

        if not segment_paths:
            self._active_segment_id = 1
            self._active_handle = open(
                self.data_dir / self._segment_filename(self._active_segment_id),
                "ab+",
            )
            self._active_handle.seek(0, os.SEEK_END)
            return

        for segment_path in segment_paths:
            segment_id = self._segment_id_from_path(segment_path)
            for offset, flag, key, value in self._iter_segment_records(segment_path):
                if flag == FLAG_SET:
                    self._index[key] = RecordPointer(segment_id, offset)
                    self._add_secondary_entries(key, value)
                elif flag == FLAG_DELETE:
                    self._index.pop(key, None)
                    self._remove_secondary_entries(key)

        self._active_segment_id = self._segment_id_from_path(segment_paths[-1])
        active_path = self.data_dir / self._segment_filename(self._active_segment_id)
        self._active_handle = open(active_path, "ab+")
        self._active_handle.seek(0, os.SEEK_END)

        if self._active_handle.tell() >= self.segment_size_limit:
            self._rotate_segment()

    def _append_record(self, key: str, value: Any, flag: int) -> int:
        if self._active_handle is None or self._active_handle.closed:
            raise StorageEngineError("Storage engine is closed.")

        estimated_size = self._estimate_record_size(key, value, flag)
        current_size = self._active_handle.tell()
        if current_size + estimated_size > self.segment_size_limit:
            self._rotate_segment()

        offset = self._write_record(self._active_handle, key, value, flag)
        self._active_handle.flush()
        if self.sync_on_write:
            os.fsync(self._active_handle.fileno())
        return offset

    def _write_record(
        self,
        handle,
        key: str,
        value: Any,
        flag: int,
    ) -> int:
        key_bytes = key.encode("utf-8")
        value_bytes = b""

        if flag == FLAG_SET:
            try:
                value_bytes = json.dumps(value, separators=(",", ":")).encode("utf-8")
            except TypeError as exc:
                raise StorageEngineError(
                    f"Value for key '{key}' is not JSON serializable."
                ) from exc

        payload = key_bytes + value_bytes
        checksum = zlib.crc32(bytes([flag]) + payload) & 0xFFFFFFFF
        header = HEADER.pack(MAGIC, flag, len(key_bytes), len(value_bytes), checksum)

        handle.seek(0, os.SEEK_END)
        offset = handle.tell()
        handle.write(header)
        handle.write(payload)
        return offset

    def _read_record(self, segment_id: int, offset: int) -> tuple[int, str, Any]:
        segment_path = self.data_dir / self._segment_filename(segment_id)
        with open(segment_path, "rb") as handle:
            handle.seek(offset)
            record = self._read_record_from_handle(handle)
            if record is None:
                raise CorruptionError(
                    f"Record at offset {offset} in {segment_path.name} is incomplete."
                )
            return record

    def _iter_segment_records(
        self,
        segment_path: Path,
    ) -> Iterator[tuple[int, int, str, Any]]:
        with open(segment_path, "rb") as handle:
            while True:
                offset = handle.tell()
                record = self._read_record_from_handle(handle, allow_partial_tail=True)
                if record is None:
                    break
                flag, key, value = record
                yield offset, flag, key, value

    def _read_record_from_handle(
        self,
        handle,
        allow_partial_tail: bool = False,
    ) -> tuple[int, str, Any] | None:
        header_bytes = handle.read(HEADER.size)
        if not header_bytes:
            return None

        if len(header_bytes) < HEADER.size:
            if allow_partial_tail:
                return None
            raise CorruptionError("Incomplete record header encountered.")

        magic, flag, key_len, value_len, checksum = HEADER.unpack(header_bytes)
        if magic != MAGIC:
            raise CorruptionError("Invalid record magic bytes encountered.")

        payload_size = key_len + value_len
        payload = handle.read(payload_size)
        if len(payload) < payload_size:
            if allow_partial_tail:
                return None
            raise CorruptionError("Incomplete record payload encountered.")

        actual_checksum = zlib.crc32(bytes([flag]) + payload) & 0xFFFFFFFF
        if checksum != actual_checksum:
            raise CorruptionError("Checksum mismatch detected while reading segment.")

        key = payload[:key_len].decode("utf-8")
        value_bytes = payload[key_len:]

        if flag == FLAG_DELETE:
            return flag, key, None
        if flag != FLAG_SET:
            raise CorruptionError(f"Unknown record flag encountered: {flag}")

        value = json.loads(value_bytes.decode("utf-8"))
        return flag, key, value

    def _rotate_segment(self) -> None:
        self.close()
        self._active_segment_id += 1
        active_path = self.data_dir / self._segment_filename(self._active_segment_id)
        self._active_handle = open(active_path, "ab+")
        self._active_handle.seek(0, os.SEEK_END)

    def _segment_paths(self) -> list[Path]:
        return sorted(self.data_dir.glob("segment-*.db"))

    def _segment_filename(self, segment_id: int) -> str:
        return SEGMENT_TEMPLATE.format(segment_id=segment_id)

    def _segment_id_from_path(self, path: Path) -> int:
        return int(path.stem.split("-")[1])

    def _estimate_record_size(self, key: str, value: Any, flag: int) -> int:
        key_bytes = key.encode("utf-8")
        value_bytes = b""
        if flag == FLAG_SET:
            value_bytes = json.dumps(value, separators=(",", ":")).encode("utf-8")
        return HEADER.size + len(key_bytes) + len(value_bytes)

    def _validate_key(self, key: str) -> None:
        if not isinstance(key, str) or not key:
            raise StorageEngineError("Keys must be non-empty strings.")

    def _resolve_indexed_fields(
        self,
        indexed_fields: tuple[str, ...] | list[str] | None,
    ) -> tuple[str, ...]:
        if indexed_fields is None:
            metadata = self._load_metadata()
            indexed_fields = metadata.get("indexed_fields", [])

        cleaned = sorted({field for field in indexed_fields if isinstance(field, str) and field})
        return tuple(cleaned)

    def _metadata_path(self) -> Path:
        return self.data_dir / METADATA_FILE

    def _load_metadata(self) -> dict[str, Any]:
        metadata_path = self._metadata_path()
        if not metadata_path.exists():
            return {}
        with open(metadata_path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def _save_metadata(self) -> None:
        metadata = {"indexed_fields": list(self.indexed_fields)}
        with open(self._metadata_path(), "w", encoding="utf-8") as handle:
            json.dump(metadata, handle, indent=2)

    def _extract_index_tokens(self, value: Any) -> dict[str, str]:
        if not isinstance(value, dict):
            return {}

        tokens: dict[str, str] = {}
        for field in self.indexed_fields:
            if field in value:
                tokens[field] = self._encode_index_value(value[field])
        return tokens

    def _encode_index_value(self, value: Any) -> str:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))

    def _add_secondary_entries(self, key: str, value: Any) -> None:
        self._remove_secondary_entries(key)
        tokens = self._extract_index_tokens(value)
        if not tokens:
            self._indexed_values_by_key.pop(key, None)
            return

        self._indexed_values_by_key[key] = tokens
        for field, token in tokens.items():
            self._secondary_indexes[field].setdefault(token, set()).add(key)

    def _remove_secondary_entries(self, key: str) -> None:
        old_tokens = self._indexed_values_by_key.pop(key, None)
        if not old_tokens:
            return

        for field, token in old_tokens.items():
            keys = self._secondary_indexes[field].get(token)
            if keys is None:
                continue
            keys.discard(key)
            if not keys:
                self._secondary_indexes[field].pop(token, None)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Custom key-value storage engine")
    parser.add_argument(
        "--data-dir",
        default="07_database_storage/data/cli_store",
        help="Directory where segment files are stored",
    )
    parser.add_argument(
        "--segment-size-limit",
        type=int,
        default=1024 * 1024,
        help="Maximum size of one segment file in bytes",
    )
    parser.add_argument(
        "--sync-on-write",
        action="store_true",
        help="Call fsync after each write",
    )
    parser.add_argument(
        "--indexed-fields",
        default=None,
        help="Comma-separated top-level JSON fields to maintain as secondary indexes",
    )

    subparsers = parser.add_subparsers(dest="command")

    set_parser = subparsers.add_parser("set", help="Insert or update a key")
    set_parser.add_argument("key")
    set_parser.add_argument("value", help="JSON value, for example '{\"name\":\"Aarav\"}'")

    get_parser = subparsers.add_parser("get", help="Read a key")
    get_parser.add_argument("key")

    delete_parser = subparsers.add_parser("delete", help="Delete a key")
    delete_parser.add_argument("key")

    scan_parser = subparsers.add_parser("scan", help="Scan keys in sorted order")
    scan_parser.add_argument("--prefix", default=None)

    range_parser = subparsers.add_parser("range-scan", help="Scan a key range")
    range_parser.add_argument("--start", default=None)
    range_parser.add_argument("--end", default=None)

    query_parser = subparsers.add_parser("query", help="Query by indexed field")
    query_parser.add_argument("field")
    query_parser.add_argument("value", help="Field value, parsed as JSON when possible")

    subparsers.add_parser("compact", help="Rewrite live keys into a fresh segment")
    subparsers.add_parser("stats", help="Show engine stats")
    subparsers.add_parser("demo", help="Run the built-in demo")

    return parser


def parse_json_value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def parse_indexed_fields(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    return [field.strip() for field in raw.split(",") if field.strip()]


def print_records(records: Iterator[tuple[str, Any]]) -> None:
    found_any = False
    for key, value in records:
        found_any = True
        print(f"{key} -> {json.dumps(value, sort_keys=True)}")
    if not found_any:
        print("(no records)")


def run_cli(argv: list[str]) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.command in (None, "demo"):
        demo()
        return 0

    engine = StorageEngine(
        args.data_dir,
        segment_size_limit=args.segment_size_limit,
        sync_on_write=args.sync_on_write,
        indexed_fields=parse_indexed_fields(args.indexed_fields),
    )

    try:
        if args.command == "set":
            engine.set(args.key, parse_json_value(args.value))
            print(f"stored {args.key}")
            return 0

        if args.command == "get":
            print(json.dumps(engine.get(args.key), indent=2, sort_keys=True))
            return 0

        if args.command == "delete":
            deleted = engine.delete(args.key)
            print("deleted" if deleted else "not found")
            return 0

        if args.command == "scan":
            print_records(engine.scan(prefix=args.prefix))
            return 0

        if args.command == "range-scan":
            print_records(engine.range_scan(start_key=args.start, end_key=args.end))
            return 0

        if args.command == "query":
            print_records(engine.query_by_field(args.field, parse_json_value(args.value)))
            return 0

        if args.command == "compact":
            before = engine.stats()
            engine.compact()
            after = engine.stats()
            print(json.dumps({"before": before, "after": after}, indent=2, sort_keys=True))
            return 0

        if args.command == "stats":
            print(json.dumps(engine.stats(), indent=2, sort_keys=True))
            return 0

        parser.error(f"Unknown command: {args.command}")
        return 2

    finally:
        engine.close()


def demo() -> None:
    demo_dir = Path(__file__).resolve().parent / "data" / "demo_store"
    if demo_dir.exists():
        shutil.rmtree(demo_dir)

    with StorageEngine(
        demo_dir,
        segment_size_limit=220,
        indexed_fields=("tier", "customer_id"),
    ) as engine:
        engine.set("user:1", {"name": "Aarav", "tier": "gold", "balance": 1200})
        engine.set("user:2", {"name": "Diya", "tier": "silver", "balance": 650})
        engine.set("order:1001", {"customer_id": "user:1", "amount": 299.99})
        engine.set("user:1", {"name": "Aarav", "tier": "platinum", "balance": 1500})
        engine.delete("user:2")

        print("Before restart:")
        print(json.dumps(engine.stats(), indent=2, sort_keys=True))
        print("Range scan user:1..user:9")
        print_records(engine.range_scan(start_key="user:1", end_key="user:9"))
        print("Query tier=platinum")
        print_records(engine.query_by_field("tier", "platinum"))

    with StorageEngine(demo_dir, segment_size_limit=220) as engine:
        print("\nRecovered after restart:")
        print(json.dumps(engine.stats(), indent=2, sort_keys=True))
        print("user:1 =", json.dumps(engine.get("user:1"), sort_keys=True))
        print("user:2 exists =", engine.exists("user:2"))
        print("Query customer_id=user:1")
        print_records(engine.query_by_field("customer_id", "user:1"))

        engine.compact()
        print("\nAfter compaction:")
        print(json.dumps(engine.stats(), indent=2, sort_keys=True))
        print_records(engine.scan(prefix="user:"))


if __name__ == "__main__":
    try:
        raise SystemExit(run_cli(sys.argv[1:]))
    except StorageEngineError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
