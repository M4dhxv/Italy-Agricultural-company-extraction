"""
failure_logger.py — Append-only failure logging to failures.csv.
"""
from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from pathlib import Path

from src.config import FAILURES_CSV_PATH

_FIELDNAMES = [
    "company_name",
    "stage",
    "source",
    "error_type",
    "error_message",
    "timestamp",
]


def log_failure(
    company_name: str,
    stage: str,
    source: str,
    error_type: str,
    error_message: str,
) -> None:
    """Append one failure row to failures.csv (creates file + header if absent)."""
    path = Path(FAILURES_CSV_PATH)
    write_header = not path.exists() or path.stat().st_size == 0

    row = {
        "company_name": company_name,
        "stage": stage,
        "source": source,
        "error_type": error_type,
        "error_message": str(error_message)[:500],  # cap long tracebacks
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerow(row)
