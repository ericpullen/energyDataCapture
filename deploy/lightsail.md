# Running the collector on AWS Lightsail

The lift-and-shift of `energycap run` off the Mac Mini, deployed 2026-08-19. This is
**phase 1 only**: the collector polls and spools exactly as it does at home, and
nothing touches S3. `deploy/README.md` still describes the Apple `container` + launchd
path on the Mac; this file describes the cloud host.

## What is deployed

| | |
|---|---|
| Instance | `energycap`, `us-east-1a`, blueprint `ubuntu_24_04`, bundle `micro_3_0` |
| Size | 1 GB RAM, 2 vCPU, 40 GB SSD, 2 TB transfer — **$7/month**, all in |
| Static IP | `energycap-ip` → `13.219.164.226` (included in the bundle) |
| Firewall | TCP 22 and 8080, both from `99.119.39.143/32` only |
| Runtime | Docker 29.7.2 + Compose v5.5.0, `restart: unless-stopped` |
| Repo | `git clone https://github.com/ericpullen/energyDataCapture` (public) |
| Spool | Docker **named volume** `energycap-data`, not a bind mount |

`us-east-1` because the legacy `bryant-energy-data` DynamoDB table lives there, and the
future S3 bucket should sit alongside it.

## Why the spool is a named volume

The named volume from `docker-compose.yml` is doing real work here: it puts the SQLite
spool somewhere the host filesystem cannot casually reach. Opening `spool.db` from the
host while the container writes corrupted it **twice** on the Mac (STATE.md, "The spool
was corrupted a second time"), including once through a read-only call. Keep it that
way — reach the spool only through the running process:

```bash
docker compose exec energycap energycap <subcommand>
```

## Reaching the dashboard

8080 is published on all interfaces and opened in the Lightsail firewall to the house
IP only, so the dashboard is reachable directly:

```bash
open http://13.219.164.226:8080/ui
```

**The firewall rule is the only thing protecting it.** The health server binds `0.0.0.0`
and has no authentication, so anything that reaches the port gets the full dashboard —
live circuit-level load, which is a fairly direct occupancy signal. Chosen deliberately
on 2026-08-19 for simplicity against a static home IP, and cheap to reverse: drop the
8080 rule, put `ports: !override` / `- "127.0.0.1:8080:8080"` back in the compose
override, and tunnel instead:

```bash
ssh -f -N -i ~/.ssh/energycap-lightsail.pem -L 8090:127.0.0.1:8080 ubuntu@13.219.164.226
open http://localhost:8090/ui     # 8090 because 8080 on the Mac is the local collector
```

Close a tunnel with `pkill -f 'ssh -f -N.*13.219.164.226'`.

The private key is at `~/.ssh/energycap-lightsail.pem`, mode 600. It is **not** in this
repo and must never be. Lightsail cannot re-issue it — if it is lost, create a new key
pair and rebuild the instance.

The home IP is static, so the CIDR should hold. If it ever does change, both SSH and the
dashboard stop answering at once — re-point both rules together, since
`put-instance-public-ports` **replaces** the entire rule set rather than adding to it:

```bash
HOME_IP=$(curl -s https://checkip.amazonaws.com)/32
aws lightsail put-instance-public-ports --region us-east-1 --instance-name energycap \
  --port-infos fromPort=22,toPort=22,protocol=TCP,cidrs=$HOME_IP \
               fromPort=8080,toPort=8080,protocol=TCP,cidrs=$HOME_IP
```

## Host-specific configuration

`docker-compose.override.yml` on the instance is deliberately **untracked** — it
describes that host, not the project. Compose merges it automatically. It mounts the
blackstart inventory and repoints `BLACKSTART_INVENTORY_PATH` at the mount; the port
binding is left to the base compose file.

```yaml
services:
  energycap:
    volumes:
      - /home/ubuntu/inventory/montfort.json:/inventory/montfort.json:ro
    environment:
      BLACKSTART_INVENTORY_PATH: /inventory/montfort.json
```

If you ever put a `ports:` entry back, tag it `!override`. Compose *merges* port lists
rather than replacing them, so a plain `- "127.0.0.1:8080:8080"` leaves you bound to
`0.0.0.0:8080` **as well** — the opposite of what you asked for, and silently. Check with
`docker compose config | grep -A6 ports:` after any change.

The inventory mount is not optional cosmetics: the dashboard joins it at runtime, and
without it every breaker shows as a bare `breaker_p19` instead of "Water heater".

## What was deliberately NOT copied

- **`data/tokens/*.json`.** Okta rotates the Carrier refresh token on every refresh
  (`sources/carrier_auth.py:1291`), so two hosts sharing one cached chain take turns
  invalidating each other. Each host bootstraps its own from `.env`. The same reasoning
  applies to `tokens/lge.json`.
- **`S3_BUCKET`.** Present in `.env` but empty, so `upload_hourly`, `rollup_hourly` and
  `daily_maintenance` fail cleanly as `job_failed` and the poll loops are untouched.
  That is the intended phase-1 behaviour, not a defect.
- **`greenbutton_daily`** will fail nightly at 09:15 for want of a token cache. Expected;
  LG&E stays on the Mac.
- **The spool.** The instance started empty on purpose — see the handoff below.

## Recreating it from scratch

```bash
source ~/code/bryantDeployerRole.sh    # IAM user bryantDataCollectorDeployer

aws lightsail create-key-pair --region us-east-1 --key-pair-name energycap-key \
  --query privateKeyBase64 --output text > ~/.ssh/energycap-lightsail.pem
chmod 600 ~/.ssh/energycap-lightsail.pem

aws lightsail create-instances --region us-east-1 --instance-names energycap \
  --availability-zone us-east-1a --blueprint-id ubuntu_24_04 --bundle-id micro_3_0 \
  --key-pair-name energycap-key --user-data "$(cat deploy/lightsail-userdata.sh)" \
  --tags key=project,value=energycap

aws lightsail allocate-static-ip --region us-east-1 --static-ip-name energycap-ip
aws lightsail attach-static-ip   --region us-east-1 --static-ip-name energycap-ip \
  --instance-name energycap
aws lightsail put-instance-public-ports --region us-east-1 --instance-name energycap \
  --port-infos fromPort=22,toPort=22,protocol=TCP,cidrs=$(curl -s https://checkip.amazonaws.com)/32
```

`put-instance-public-ports` **replaces** the whole rule set, which is how the default
80/443 rules get removed. Then, once `/var/lib/energycap-userdata-done` exists:

```bash
scp -i ~/.ssh/energycap-lightsail.pem .env ubuntu@<ip>:~/energyDataCapture/.env
ssh ... "chmod 600 ~/energyDataCapture/.env"
scp -i ~/.ssh/energycap-lightsail.pem ~/code/blackstart/data/montfort.json ubuntu@<ip>:~/inventory/
# write docker-compose.override.yml (above), then:
ssh ... "cd ~/energyDataCapture && docker compose build && docker compose up -d"
```

## Operating it

```bash
ssh -i ~/.ssh/energycap-lightsail.pem ubuntu@13.219.164.226
cd ~/energyDataCapture

docker compose ps
docker compose logs -f --tail 50
docker compose restart
docker compose exec energycap energycap discover     # any stage, inside the process

git pull && docker compose build && docker compose up -d   # deploy a new revision
```

Logs are capped by compose at 10 MB × 5 files, and journald at 500 MB.

## The cutover from the Mac — done 2026-08-19

The Mac's whole history moved to the instance and the Mac collector was stopped. Final
state: **181,371 rows** spanning `2026-08-17T19:22:55Z .. 2026-08-19T15:55:06Z`,
14 channels, `integrity_check` ok. There is one collector again.

The two collectors had overlapped for ~25 minutes, which is the part that needed care.
**The overlap was dropped, not merged.** Both were sampling the same readings on
different clocks, so a union would have given each channel ~240 samples in that hour
instead of ~120 — and since `kwh = mean_watts * sample_count * poll_interval_s / 3.6e6`,
that doubles every kWh figure for the overlap. The dedupe key cannot save you here:
`ts_utc` differs, so the rows are not duplicates by the key even though they are
duplicates in fact.

What *was* carried across is the instance's rows strictly **after** the Mac's last
timestamp — 102 of them, covering a window the Mac never saw. That gives no double count
and no hole: the boundary went `15:53:43.475Z -> 15:54:05.761Z`, a 22-second step, under
one poll interval.

The procedure, if it is ever needed again:

```bash
# on the Mac -- a clean stop checkpoints the WAL, after which spool.db is
# self-contained and the -wal/-shm are gone. Verify BEFORE copying.
./scripts/energycap-container.sh stop
sqlite3 data/spool.db "PRAGMA integrity_check;"
scp -i ~/.ssh/energycap-lightsail.pem data/spool.db ubuntu@<ip>:/tmp/mac-spool.db

# on the instance -- stop first; the volume must not be written while spliced
docker compose stop
sudo python3 /tmp/splice.py        # see the docstring; drops overlap, keeps the tail
VOL=/var/lib/docker/volumes/energycap-data/_data
sudo cp $VOL/spool.db /tmp/instance-soak-spool.db.bak
sudo rm -f $VOL/spool.db-wal $VOL/spool.db-shm      # stale WAL against a new file
sudo cp /tmp/merged-spool.db $VOL/spool.db
sudo chown 10001:10001 $VOL/spool.db                # container runs non-root
docker compose up -d
```

Leave `tokens/` and `status.json` in the volume alone — only `spool.db` is replaced.
Deleting the stale `-wal`/`-shm` matters: SQLite would otherwise try to apply a WAL
belonging to the file you just overwrote.

The Mac's `data/spool.db` is untouched on that machine and is the pre-migration backup.
Keep it until the instance has a few days of clean running behind it.

## Cost

$7.00/month flat — instance, 40 GB SSD, static IP and 2 TB transfer included. Compare
EC2 `t4g.micro` at ~$11.40 (compute $6.13 + 20 GB gp3 $1.60 + public IPv4 $3.65).

The tradeoff is that Lightsail instances take no IAM instance profile, so the S3 phase
will need a scoped access key on the box rather than a role. That is the same posture as
the four API credentials already in `.env`, and the key can be limited to `PutObject` on
one prefix.
