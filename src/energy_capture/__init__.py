"""energycap — household energy + HVAC time-series capture pipeline.

Module map (see PLAN.md §5):

* :mod:`energy_capture.config`   — every knob, as an env var (pydantic-settings).
* :mod:`energy_capture.logging`  — structured JSON logs to stdout + credential scrubbing.
* :mod:`energy_capture.timeutil` — the ONLY home of UTC<->local conversion and
  local-date partition math.
* :mod:`energy_capture.model`    — the canonical ``Observation`` row, the Arrow
  schemas, and the sort/dedupe key constants used everywhere.

Submodules are imported explicitly (``from energy_capture import timeutil``);
this package deliberately re-exports nothing but ``__version__`` so that importing
the package never pulls in pyarrow/duckdb/boto3.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
