"""Pipeline stages (PLAN.md §5, §10).

One module per stage, each exposing a plain callable that ``energy_capture.cli``
invokes. Every stage is idempotent over an arbitrary ``--start/--end`` **local**
date range and writes deterministic filenames, so a re-run overwrites instead of
duplicating (CLAUDE.md rule 7):

============================  ===========================================
module                        stage
============================  ===========================================
``stages.poller``             30s poll loop -> SQLite spool
``stages.uploader``           spool -> ``part-{YYYYMMDD}T{HH}.parquet``
``stages.compactor``          parts -> ``day-{YYYYMMDD}.parquet``
``stages.rollup``             raw -> ``rollup-{YYYYMMDD}.parquet``
``stages.daily``              Bryant daily energy -> ``energy/daily``
``stages.backfill``           DynamoDB + legacy JSON -> ``energy/daily``
``stages.discover``           enumerate live channels (PLAN.md §9)
``stages.dim``                channel_map + inventory -> ``dim_channel``
============================  ===========================================

``energycap run`` is deliberately **not** in this package: it does not process a
date range, it *drives* the stages above (poll loops, scheduler, health server,
signal handling). It lives at :mod:`energy_capture.runtime`.

The exact entry-point names the CLI calls are listed in
``energy_capture.cli.STAGE_ENTRYPOINTS``; that table is the contract between
this package and the CLI. Modules are imported **lazily** by the CLI, so a stage
that has not landed yet degrades to a clear "not implemented yet" message rather
than breaking every other command.
"""

from __future__ import annotations

__all__: list[str] = []
