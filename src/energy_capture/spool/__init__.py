"""The crash-durable SQLite spool between the poll loop and S3 (PLAN.md §5, §10).

``from energy_capture.spool import SpoolDB`` — see
:mod:`energy_capture.spool.sqlite` for the storage contract, the durability
settings, and the concurrency model.
"""

from __future__ import annotations

from energy_capture.spool.sqlite import (
    SCHEMA_VERSION,
    PendingHour,
    SpoolDB,
    SpoolRow,
    SpoolStats,
    open_spool,
)

__all__ = [
    "SCHEMA_VERSION",
    "PendingHour",
    "SpoolDB",
    "SpoolRow",
    "SpoolStats",
    "open_spool",
]
