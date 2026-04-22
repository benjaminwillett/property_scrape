#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

from decision_engine.sources.stub import StubAdapter
from decision_engine.storage import init_db, connect
from decision_engine.sources.csv_adapter import CSVBedroomsAdapter

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Ingestion runner for decision engine")
    ap.add_argument("--source", required=True, choices=["stub", "csv_bedrooms"], help="Data source to ingest")
    ap.add_argument("--db", default="decision_engine.sqlite", help="SQLite DB path")
    ap.add_argument("--snapshot-dir", default="snapshots", help="Where raw snapshots are stored")
    ap.add_argument("--state", default=None, help="State filter (e.g. VIC)")
    ap.add_argument("--since", default=None, help="ISO date/period filter (YYYY-MM)")
    ap.add_argument("--csv-path", default=None, help="CSV path for csv_bedrooms source")
    return ap.parse_args()


def main():
    args = parse_args()
    os.makedirs(args.snapshot_dir, exist_ok=True)
    init_db(args.db)

    if args.source == "csv_bedrooms":
        if not args.csv_path:
            raise SystemExit("--csv-path is required for csv_bedrooms source")
        adapter = CSVBedroomsAdapter(args.csv_path)
    else:
        adapter = StubAdapter()
    with connect(args.db) as conn:
        run = adapter.ingest(conn, state=args.state, since=args.since, snapshot_dir=args.snapshot_dir)
        conn.commit()
    print(f"[DONE] {run.source} status={run.status} rows={run.row_count} message={run.message}")


if __name__ == "__main__":
    main()
