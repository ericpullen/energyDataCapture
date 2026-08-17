"""AWS adapters: S3 object I/O and (elsewhere) Glue table management.

* :mod:`energy_capture.aws.s3io` — the atomic Parquet writer, the row-count
  verify gate, and **the single source of truth for the S3 key layout**
  (PLAN.md §4). No other module formats an S3 key by hand.
* :mod:`energy_capture.aws.glue` — Glue table create/update (PLAN.md §12).

Nothing is imported here: ``boto3`` must not be pulled in just because something
touched :mod:`energy_capture.aws`. Import the submodule you need.
"""

from __future__ import annotations

__all__: list[str] = []
