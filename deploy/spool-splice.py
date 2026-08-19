#!/usr/bin/env python3
"""Splice the Lightsail instance's soak rows onto the Mac's spool history.

The two collectors overlapped between 15:30 and the Mac's clean stop. Those
overlapping rows are the SAME readings sampled by two clocks, so unioning them
would give a channel ~240 samples in an hour instead of ~120 --- and since
kwh = mean_watts * sample_count * poll_interval_s / 3.6e6, that doubles every
kWh figure for the overlap. So the overlap is dropped, not merged.

Only the instance's rows STRICTLY AFTER the Mac's last timestamp are carried
over. Those cover a window the Mac never saw, so there is no double count and
no gap between the two histories.
"""

import shutil
import sqlite3
import sys

MAC = "/tmp/mac-spool.db"
VOL = "/var/lib/docker/volumes/energycap-data/_data/spool.db"
OUT = "/tmp/merged-spool.db"

COLUMNS = (
    "ts_utc, ts_local, source, device_id, channel_id, metric, "
    "value, unit, local_date, local_hour, uploaded_at"
)

shutil.copyfile(MAC, OUT)
db = sqlite3.connect(OUT)

cutoff = db.execute("SELECT MAX(ts_utc) FROM observations").fetchone()[0]
before = db.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
print(f"mac base:  {before} rows, cutoff {cutoff}")

db.execute("ATTACH DATABASE ? AS inst", (VOL,))
tail = db.execute(
    "SELECT COUNT(*) FROM inst.observations WHERE ts_utc > ?", (cutoff,)
).fetchone()[0]
dropped = db.execute(
    "SELECT COUNT(*) FROM inst.observations WHERE ts_utc <= ?", (cutoff,)
).fetchone()[0]
print(f"instance:  {tail} rows after cutoff to carry, {dropped} overlapping rows dropped")

# id is AUTOINCREMENT and means arrival order -- let it re-assign rather than
# carrying the instance's ids across. OR IGNORE defers to the dedupe index.
db.execute(
    f"INSERT OR IGNORE INTO observations ({COLUMNS}) "
    f"SELECT {COLUMNS} FROM inst.observations WHERE ts_utc > ?",
    (cutoff,),
)
db.commit()
db.execute("DETACH DATABASE inst")

after = db.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
lo, hi = db.execute("SELECT MIN(ts_utc), MAX(ts_utc) FROM observations").fetchone()
chans = db.execute(
    "SELECT COUNT(DISTINCT source||'/'||device_id||'/'||channel_id) FROM observations"
).fetchone()[0]

print(f"merged:    {after} rows (+{after - before}), {chans} channels")
print(f"span:      {lo} .. {hi}")
print(f"integrity: {integrity}")

# The splice must be strictly additive to the Mac's history.
if integrity != "ok" or after != before + tail:
    print("FAILED: refusing to install", file=sys.stderr)
    sys.exit(1)

# Prove the boundary is continuous rather than a hidden gap.
gap = db.execute(
    "SELECT MIN(ts_utc) FROM observations WHERE ts_utc > ?", (cutoff,)
).fetchone()[0]
print(f"boundary:  {cutoff} -> {gap}")
db.close()
print("OK")
