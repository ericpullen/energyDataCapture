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
| Firewall | TCP 22 from `99.119.39.143/32` only. **8080 is not open.** |
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

The health server binds `0.0.0.0` inside the container and has **no authentication**,
so the compose override publishes it on `127.0.0.1` only and the Lightsail firewall
leaves 8080 shut. Reach it with a tunnel:

```bash
ssh -f -N -i ~/.ssh/energycap-lightsail.pem -L 8090:127.0.0.1:8080 ubuntu@13.219.164.226
open http://localhost:8090/ui
```

Port 8090 because 8080 on the Mac is the local collector. Close it with
`pkill -f 'ssh -f -N.*13.219.164.226'`.

The private key is at `~/.ssh/energycap-lightsail.pem`, mode 600. It is **not** in this
repo and must never be. Lightsail cannot re-issue it — if it is lost, create a new key
pair and rebuild the instance.

The firewall CIDR is a residential IP and **will drift**. When SSH starts timing out:

```bash
aws lightsail put-instance-public-ports --region us-east-1 --instance-name energycap \
  --port-infos fromPort=22,toPort=22,protocol=TCP,cidrs=$(curl -s https://checkip.amazonaws.com)/32
```

## Host-specific configuration

`docker-compose.override.yml` on the instance is deliberately **untracked** — it
describes that host, not the project. Compose merges it automatically. It does three
things: mounts the blackstart inventory, repoints `BLACKSTART_INVENTORY_PATH` at the
mount, and replaces the port binding with a loopback one.

Note the `!override` tag on `ports:`. Without it Compose *merges* port lists rather than
replacing them, and you end up bound to `0.0.0.0:8080` **as well as** `127.0.0.1:8080`.
Verify with `docker compose config | grep -A6 ports:` after any change.

```yaml
services:
  energycap:
    volumes:
      - /home/ubuntu/inventory/montfort.json:/inventory/montfort.json:ro
    environment:
      BLACKSTART_INVENTORY_PATH: /inventory/montfort.json
    ports: !override
      - "127.0.0.1:8080:8080"
```

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

## Cutting over from the Mac

Both collectors currently run in parallel. That is safe for the *data* — the spools are
separate and nothing merges them — but the upstream is shared, so it is a soak test, not
a steady state. Two things to watch: the Leviton hub now receives `bandwidth: 1` from two
processes (never `bandwidth: 0`, so it should be benign but is unproven), and each host
holds an independent Carrier refresh chain.

When ready to hand off:

```bash
# on the Mac — clean stop checkpoints the WAL
./scripts/energycap-container.sh stop

# copy all three files; spool.db alone is ~4 KB and the data is in the -wal
scp -i ~/.ssh/energycap-lightsail.pem data/spool.db* ubuntu@13.219.164.226:/tmp/

# on the instance: stop, replace the volume contents, restart
docker compose stop
docker run --rm -v energycap-data:/data -v /tmp:/in alpine \
  sh -c 'cp /in/spool.db* /data/ && chown 10001:10001 /data/spool.db*'
docker compose up -d
```

Discard whatever the instance spooled during the soak rather than merging it — those
rows are duplicate readings of what the Mac already captured, stamped with a different
`ts_utc`, and merging them would inflate `sample_count` and therefore every kWh figure.

## Cost

$7.00/month flat — instance, 40 GB SSD, static IP and 2 TB transfer included. Compare
EC2 `t4g.micro` at ~$11.40 (compute $6.13 + 20 GB gp3 $1.60 + public IPv4 $3.65).

The tradeoff is that Lightsail instances take no IAM instance profile, so the S3 phase
will need a scoped access key on the box rather than a role. That is the same posture as
the four API credentials already in `.env`, and the key can be limited to `PutObject` on
one prefix.
