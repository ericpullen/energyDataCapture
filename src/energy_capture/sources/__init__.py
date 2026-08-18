"""Cloud sources: one module per upstream API, all behind :mod:`~.base`.

``sources/leviton.py`` and ``sources/bryant.py`` are thin **adapters** over
their third-party clients (``aioleviton``, and likely ``carrier-api``) so a
stale upstream can be vendored or replaced without touching the pipeline
(CLAUDE.md, "Architecture"). Nothing outside this package imports those clients.

Re-exported here so callers can write ``from energy_capture.sources import
Source, PollCycle`` without depending on the module layout.
"""

from __future__ import annotations

from energy_capture.sources.base import (
    BackgroundTask,
    BaseSource,
    DiscoveredChannel,
    DiscoveredDevice,
    Discovery,
    PollCycle,
    Source,
    SourceAuthError,
    SourceError,
    SourceTransientError,
)

__all__ = [
    "BackgroundTask",
    "BaseSource",
    "DiscoveredChannel",
    "DiscoveredDevice",
    "Discovery",
    "PollCycle",
    "Source",
    "SourceAuthError",
    "SourceError",
    "SourceTransientError",
]
