"""
state_manager.py — Atomic read/write of state.json for resume support.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from src.config import STATE_PATH


def load_state() -> dict:
    """Return the persisted pipeline state, or an empty default."""
    path = Path(STATE_PATH)
    if not path.exists():
        return {"last_processed_company": None, "index": -1}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"last_processed_company": None, "index": -1}


def save_state(company_name: str, index: int) -> None:
    """Atomically write state to disk (temp-file + rename)."""
    payload = {"last_processed_company": company_name, "index": index}
    path = Path(STATE_PATH)
    # Write to a sibling temp file first, then rename atomically
    tmp_fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def reset_state() -> None:
    """Delete state file to force a fresh run."""
    path = Path(STATE_PATH)
    if path.exists():
        path.unlink()
