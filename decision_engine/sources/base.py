from __future__ import annotations

import datetime as dt
import json
import uuid
from abc import ABC, abstractmethod
from typing import Iterable, List, Optional, Tuple

from ..models import DataQuality, IngestionRun, MetricRecord, Suburb
from ..storage import bulk_upsert_metrics, record_run, upsert_quality, upsert_suburb


class SourceAdapter(ABC):
    source_name: str

    @abstractmethod
    def fetch_raw(self, state: Optional[str], since: Optional[str]) -> Tuple[List[dict], List[str]]:
        """Return (raw_rows, debug_logs). Implement idempotent fetch with snapshots elsewhere."""

    @abstractmethod
    def transform(self, raw_rows: List[dict]) -> Tuple[List[Suburb], List[MetricRecord], List[DataQuality]]:
        """Normalize raw rows into reference + metric records."""

    def snapshot_path(self, snapshot_dir: str) -> str:
        stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        return f"{snapshot_dir}/{self.source_name}_{stamp}.json"

    def persist_snapshot(self, snapshot_dir: str, raw_rows: List[dict]):
        path = self.snapshot_path(snapshot_dir)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(raw_rows, f, ensure_ascii=False)
        return path

    def ingest(self, conn, state: Optional[str], since: Optional[str], snapshot_dir: str, log_fn=print) -> IngestionRun:
        run_id = str(uuid.uuid4())
        started = dt.datetime.utcnow().isoformat()
        raw_rows, debug_logs = self.fetch_raw(state=state, since=since)
        for ln in debug_logs:
            log_fn(f"[INGEST:{self.source_name}] {ln}")

        snapshot_path = self.persist_snapshot(snapshot_dir, raw_rows)
        log_fn(f"[INGEST:{self.source_name}] snapshot saved to {snapshot_path}")

        suburbs, metrics, dq_rows = self.transform(raw_rows)
        row_count = len(metrics)

        try:
            for sub in suburbs:
                upsert_suburb(conn, sub)
            bulk_upsert_metrics(conn, metrics)
            for dq in dq_rows:
                upsert_quality(conn, dq)
            status = "success"
            message = f"upserted {len(suburbs)} suburbs, {row_count} metrics"
        except Exception as exc:  # pragma: no cover - defensive logging
            status = "failed"
            message = f"failed: {exc}"
            log_fn(f"[INGEST:{self.source_name}] ERROR {exc}")
        finished = dt.datetime.utcnow().isoformat()

        run = IngestionRun(
            run_id=run_id,
            source=self.source_name,
            state=state,
            since=since,
            started_at=started,
            finished_at=finished,
            row_count=row_count,
            status=status,
            message=message,
        )
        record_run(conn, run)
        return run
