# SkySecure V3

SkySecure V3 is a real-time aircraft telemetry security research platform. It collects aircraft reports, builds one canonical track per aircraft, evaluates five detection layers, stores durable event history, and presents current evidence through a FastAPI API and browser dashboard.

> Status: engineering prototype. SkySecure does not claim certified surveillance performance, measured real-world detection accuracy, a validated false-positive rate, or operational physical MLAT performance. Public-feed agreement is not physical multilateration. Layer 3 uses a disclosed heuristic unless a separately trained checkpoint is installed. Layer 4 remains unavailable until genuine authenticated receiver evidence exists.

## Contents

- [The 60-second explanation](#the-60-second-explanation)
- [What runs today](#what-runs-today)
- [Architecture and data flow](#architecture-and-data-flow)
- [The five detection layers](#the-five-detection-layers)
- [Evidence, status, and risk](#evidence-status-and-risk)
- [Services and ownership](#services-and-ownership)
- [Kafka topics](#kafka-topics)
- [Persistence and delivery guarantees](#persistence-and-delivery-guarantees)
- [Security and trust boundaries](#security-and-trust-boundaries)
- [Requirements and configuration](#requirements-and-configuration)
- [Starting a fresh installation](#starting-a-fresh-installation)
- [Updating an existing installation safely](#updating-an-existing-installation-safely)
- [Using the dashboard](#using-the-dashboard)
- [API and WebSocket reference](#api-and-websocket-reference)
- [Layer 3 model operation](#layer-3-model-operation)
- [Layer 4 physical MLAT operation](#layer-4-physical-mlat-operation)
- [Backup, migration, rollback, and recovery](#backup-migration-rollback-and-recovery)
- [Monitoring and verification](#monitoring-and-verification)
- [Testing and development](#testing-and-development)
- [Troubleshooting](#troubleshooting)
- [Known limitations](#known-limitations)
- [Further improvements](#further-improvements)

## The 60-second explanation

Imagine several people watching the same aircraft:

1. One person reports where the aircraft says it is.
2. Another asks whether separate public aggregators—whose upstream data may overlap—tell a compatible story.
3. Another checks whether the movement is physically plausible.
4. Another learns the aircraft's recent pattern and watches its navigation-quality indicators.
5. If physical receivers are installed, another independently calculates position from arrival times and compares that result with the broadcast position.
6. A final identity check asks whether the same transponder identity appears impossibly far apart at nearly the same time.

SkySecure records which checks actually ran. A missing input is `SKIPPED`, not silently treated as safe. A trigger carries measured evidence, a detector name, event time, and a risk contribution.

Without receiver hardware, the public-feed, kinematic, trajectory-fallback, integrity-metadata, persistence, API, and dashboard paths still work. Physical Layer 4 cannot honestly become ready without real synchronized receivers.

## What runs today

| Area | Current state |
|---|---|
| Public ADS-B collection | Implemented with OpenSky/public-feed fallback behavior and runtime coverage selection |
| Canonical track fusion | Implemented with source event ordering, per-aircraft serialization, Redis state, PostgreSQL history, and Kafka outbox delivery |
| L1 public-source comparison | Implemented, rate-limited, and explicitly not physical TDOA |
| L2 physics/statistical checks | Implemented with restart-persistent baselines |
| L3 integrity | NIC and NACp integrity evidence are implemented |
| L3 trajectory | Heuristic fallback is active unless `/app/data/trajectory_lstm.pt` is installed |
| L4 receiver intake and solver | Implemented and fail-closed, but operationally unavailable without genuine physical receptions |
| L4 comparisons | Event-aligned position comparison with a threshold that respects MLAT CEP90 |
| L5 identity | Duplicate ICAO evidence is implemented |
| External threat intelligence | Not integrated into the deployed Compose runtime |
| Dashboard/API | Implemented with same-origin REST/WebSocket routing and layer evidence views |
| Scientific validation | Not complete; requires labeled field data |

The remaining work is tracked in [REMAINING_IMPROVEMENTS.md](REMAINING_IMPROVEMENTS.md).

## Architecture and data flow

```text
                              PUBLIC-FEED PATH
 OpenSky / public fallback feeds
              |
              v
       adsb-ingestor
              |
              | raw.adsb
              v
+---------+  fusion-engine  +----------------+  durable event + outbox
| Redis   |<--------------->| PostgreSQL     |<-----------------------+
| live    |                 | PostGIS/history|                        |
| state   |                 +----------------+                        |
+---------+                         |                                 |
     ^                              | fused.tracks                    |
     |                              v                                 |
     +---------------------- anomaly-detector                         |
                                  |                                  |
                                  | alerts.anomaly                    |
                                  v                                  |
                             Kafka consumers                          |
                                                                     |
                              PHYSICAL MLAT PATH                      |
 GPS-disciplined receiver A/B/C/D+                                   |
              | authenticated Mode-S receptions                      |
              v                                                      |
 POST /api/mlat/receptions                                           |
              | broker acknowledgement                               |
              | raw.mlat.receptions                                  |
              v                                                      |
         mlat-solver -- signed report --> raw.mlat -------------------+

 Redis enriched state --> FastAPI --> same-origin Nginx dashboard
                              `--> /ws/tracks
```

### End-to-end public-feed event

1. `adsb-ingestor` fetches and bounds provider records.
2. A validated `RawADSBMessage` is published to `raw.adsb` with broker acknowledgement.
3. `fusion-engine` rejects invalid time/order, updates source-owned fields, and evaluates fusion-owned L2/L4/L5 evidence.
4. PostgreSQL atomically claims the immutable source event, inserts the track point, and stores the exact `fused.tracks` outbox envelope.
5. The outbox envelope is broker-acknowledged and marked delivered by its lease owner.
6. Redis is updated only after durable persistence/publication succeeds.
7. `anomaly-detector` consumes `fused.tracks`, runs L2/L3, computes active-evidence risk, writes enriched `sv:{ICAO}` state, and publishes qualifying alert events.
8. FastAPI and the dashboard read bounded current snapshots.

### End-to-end physical MLAT event

1. A configured receiver sends an authenticated, bounded-time, valid Mode-S reception.
2. The API verifies that the credential's receiver identity equals the payload identity.
3. The API publishes to `raw.mlat.receptions` and only then marks that receiver recently active.
4. `mlat-solver` groups receptions that contain the same Mode-S payload and fit the same narrow event-time window.
5. At least four distinct configured receivers with safe geometry are required.
6. The solver produces a position, residual, CEP90, receiver identities, and one canonical source-event ID per reception.
7. The solver signs the report with the independent solver key and publishes to `raw.mlat`.
8. Fusion verifies the signature and provenance before using the report.
9. Only event-aligned ADS-B/MLAT evidence can trigger Layer 4 comparisons.

## The five detection layers

### Layer 1 — position and source validation

**Layman's terms:** ask several public flight-tracking networks whether they place the aircraft in roughly the same area. Major disagreement is suspicious, but agreement only means the aggregators agree.

**Implemented behavior:**

- Normalizes claims from OpenSky, adsb.lol, and adsb.fi.
- Caches claim-sensitive results to avoid repeated external calls.
- Rotates through a bounded live-aircraft budget instead of calling every provider for every aircraft.
- Produces verdicts such as `LEGITIMATE`, `UNCERTAIN`, `SPOOFED`, or `INSUFFICIENT_SOURCES`.
- Exposes L1 evidence through `/api/layers`, `/api/layers/L1/triggers`, and `/api/l1/validate`.

**Trust boundary:** these are aggregators, not independent RF receivers. They may share upstream data. L1 does not prove where a signal physically originated.

**What is needed to improve it:** commercial/higher-rate feeds with documented provenance, measured independence, and labeled replay datasets.

### Layer 2 — kinematic and behavioral checks

**Layman's terms:** ask whether the aircraft appears to move like a real aircraft instead of teleporting, accelerating impossibly, turning instantly, or jumping altitude.

**Implemented behavior:**

- Maximum-speed and teleportation checks.
- Altitude jump checks. Barometric-versus-geometric comparison code exists, but geometric altitude is not populated by the current public-feed adapters and that comparator is not operational on that path.
- Timestamp-aware acceleration and turn-rate checks.
- Per-aircraft velocity and vertical-rate baselines.
- ADS-B source-position conflict detection.
- Receiver-backed transponder-loss logic only when direct receiver provenance supports it.
- Redis-backed baseline persistence across detector restarts.

**Evidence lifecycle:** detector-owned L2 output is replaced each cycle rather than endlessly accumulated. A public aggregator disappearing is treated as coverage loss, not proof that a transponder was switched off.

**What is needed to improve it:** real labeled flight replay, aircraft-type-aware envelopes, weather/context inputs, and raw Mode-S ingestion.

### Layer 3 — trajectory and navigation integrity

**Layman's terms:** compare the latest motion with the aircraft's recent path and inspect the quality indicators broadcast by its navigation system.

**Implemented behavior:**

- Maintains a bounded 21-sample window: 20 inputs plus the next observation.
- Loads a two-layer LSTM checkpoint when available.
- Otherwise uses a clearly reported linear trajectory heuristic.
- Reports warm-up as `SKIPPED` until enough samples exist.
- Evaluates ADS-B NIC and NACp values for chronically low quality and changes from the aircraft's established pattern.
- Treats missing NIC/NACp as unavailable, not malicious.
- Cleans process-local history when stale tracks are pruned.

**Current mode:** unless `/app/data/trajectory_lstm.pt` exists and loads successfully, the deployed service uses `trajectory_heuristic`. The heuristic is engineering evidence, not a trained ML result.

**What is needed to improve it:** a versioned training dataset, leakage-safe train/validation/test splits, calibrated thresholds, model provenance, drift monitoring, and a deployment-approved checkpoint.

See [Layer 3 model operation](#layer-3-model-operation).

### Layer 4 — authenticated multi-sensor fusion

**Layman's terms:** calculate the aircraft's location independently from how long the same radio transmission takes to reach several receivers, then compare that independent result with what the aircraft broadcasts.

**Implemented software path:**

- Exact configured receiver identities and distinct nonempty credentials.
- Identity-bound authenticated intake.
- Valid Mode-S length/type and bounded event-time checks.
- At least four distinct receivers.
- Safe receiver geometry checks.
- Event grouping that rejects duplicate/ambiguous receiver evidence.
- TDOA solver with residual and CEP90 quality output.
- One canonical source-event ID per receiver reception.
- Independent HMAC-SHA256 solver report signature.
- Fusion-side signature, receiver identity, geometry, residual, CEP90, provenance, and time validation.
- Position comparison only inside the event-time alignment window.
- Position-disagreement threshold is at least the configured floor and expands to the report's CEP90 radius.
- Valid zero-foot MLAT altitude is preserved.
- L4 trigger evidence expires after the configured TTL.

**Fail-closed readiness:** `/api/mlat/readiness` returns HTTP 503 until enough configured receivers have recently delivered acknowledged physical receptions. ADS-B-only tracks are `SKIPPED`; the system does not fabricate MLAT evidence.

**Hardware required for operational Layer 4:** at least four geographically separated receivers, accurately surveyed locations, disciplined clocks with enough precision for TDOA, capture of the same raw Mode-S transmission, secure connectivity, and field calibration. More receivers and better geometry generally improve solve quality.

See [Layer 4 physical MLAT operation](#layer-4-physical-mlat-operation).

### Layer 5 — identity and threat intelligence

**Layman's terms:** ask whether the aircraft's claimed identity makes sense—for example, whether the same ICAO address appears hundreds of miles apart within seconds.

**Implemented deployed behavior:**

- Duplicate-ICAO position claims are serialized and compared.
- A large separation within the event window emits `duplicate_icao` evidence.
- The active evidence contributes to risk without increasing merely because the same message is replayed.

**Not currently deployed as canonical L5 services:** broad external threat feeds, sanctions/intelligence correlation, registration reconciliation, airspace policy, and the separate military-classifier module. Code existing in the repository is not described as operational unless it is connected to the Compose runtime.

## Evidence, status, and risk

Every canonical anomaly flag produced by the detector pipeline contains:

| Field | Meaning |
|---|---|
| `layer` | `L1` through `L5` |
| `detector` | Stable detector identifier |
| `type` | Anomaly category |
| `score_delta` | Current risk contribution |
| `description` | Human-readable explanation |
| `evidence` | Measurements and thresholds |
| `timestamp` | Source event time, not worker processing time |

Raw/public-feed fallback records exposed by `/api/live-aircraft` or WebSocket
fallback paths may carry only a simplified `type` and `description`. Those
display records are not canonical detector flags and must not be treated as the
full evidence contract above.

Every state vector also carries `layer_evaluations`:

- `EVALUATED`: eligible checks ran and did not trigger.
- `TRIGGERED`: one or more eligible checks triggered.
- `SKIPPED`: required evidence was missing or stale; `skipped_reason` explains why.

`GET /api/layers` keeps trigger counts in `detectors` and separately counts
checks that actually ran in `evaluated_detectors`. For L3 this makes the active
`trajectory_heuristic` or `trajectory_lstm` path and integrity coverage visible
without pretending that a skipped check evaluated the aircraft.

Risk is 0–100 and maps to:

- `NORMAL`: 0–20
- `MONITOR`: 21–50
- `ALERT`: 51–75
- `CRITICAL`: 76–100

Risk uses current unique detector evidence plus classification contribution and time decay. Repeated delivery of the same event does not intentionally ratchet risk upward.

## Services and ownership

| Service | Command/role | Owns |
|---|---|---|
| `zookeeper` | Kafka coordination | ZooKeeper state |
| `kafka` | Event transport | Topic logs and consumer offsets |
| `kafka-init` | Idempotent topic creation | Required topic/partition definitions |
| `redis` | Live low-latency state | Current tracks, coverage, baselines, MLAT accumulator, alert outbox |
| `postgres` | PostGIS durability | Track history, immutable source events, fusion outbox, registry tables |
| `postgres-migrate` | Idempotent schema migration | Existing-volume schema compatibility |
| `adsb-ingestor` | Public-feed collection | `raw.adsb` production and bounded provider snapshot state |
| `mlat-solver` | Physical reception accumulation and TDOA solving | `raw.mlat` signed reports and accumulator checkpoint |
| `fusion-engine` | Canonical source fusion | `fusion:sv:{ICAO}`, PostgreSQL event ledger/outbox, `fused.tracks` |
| `anomaly-detector` | L2/L3 and risk | `sv:{ICAO}`, L2 baselines, alert outbox, `alerts.anomaly` |
| `api` | REST, WebSocket, receiver intake, L1 | API state/caches and receiver liveness markers |
| `frontend` | Nginx static dashboard and same-origin proxy | Browser presentation only |

## Kafka topics

| Topic | Partitions | Producer | Consumer |
|---|---:|---|---|
| `raw.adsb` | 8 | `adsb-ingestor` | `fusion-engine` |
| `raw.mlat.receptions` | 8 | API receiver intake | `mlat-solver` |
| `raw.mlat` | 4 | `mlat-solver` | `fusion-engine` |
| `raw.acars` | 4 | Optional/future producer | fusion path when connected |
| `fused.tracks` | 8 | `fusion-engine` outbox | `anomaly-detector` |
| `alerts.anomaly` | 4 | `anomaly-detector` | downstream alert consumers |

Consumers manually commit only the completed record's partition at `offset + 1`. At-least-once duplicates are preferred over silent loss.

## Persistence and delivery guarantees

### PostgreSQL/PostGIS

Key tables:

- `track_points`: immutable source-event track history.
- `fusion_event_commits`: immutable source event identity and raw-event digest.
- `fusion_outbox`: exact topic/key/payload envelope, lease state, attempts, and delivery marker.
- `anomaly_events`, `aircraft_registry`, `military_icao_ranges`, and `acars_messages`: supporting schema.

For fusion events, PostgreSQL durability precedes Kafka publication and Redis mutation. The event claim, track insert, and outbox insert are one SQL operation. Duplicate source events do not create a second track row.

The outbox uses bounded periodic recovery, random lease ownership, owner-conditional delivery marking, broker acknowledgement, and per-row failure isolation. A broker failure leaves durable work retryable.

### Redis

Redis uses append-only persistence in Compose. Key discovery uses bounded rotating `SCAN` cycles and chunked pipelines; it does not use blocking `KEYS` or unbounded `scan_iter` in deployed paths.

Important namespaces include:

- `fusion:sv:{ICAO}`: fusion-owned canonical source state.
- `sv:{ICAO}`: anomaly-enriched API state.
- `baseline:l2:{ICAO}`: persistent statistical baseline.
- `mlat:accumulator`: recoverable reception grouping state.
- `mlat:receiver:last_seen:{receiver}`: acknowledged receiver liveness.
- `outbox:anomaly-alert:*`: immutable anomaly alert delivery events.

### Kafka and ZooKeeper

Fresh installations use named volumes. Existing installations that predate those declarations must adopt them only during controlled infrastructure maintenance; an application release must not recreate brokers merely to attach new volume declarations. See [docs/kafka-persistence.md](docs/kafka-persistence.md).

## Security and trust boundaries

- `.env` is ignored and must never be committed.
- Receiver credentials must exactly match configured receiver IDs; every value must be nonempty and distinct.
- Receiver credentials authenticate intake; caller-computable hashes alone are not authentication.
- The solver signing key must be independent of receiver keys.
- Fusion does not trust unsigned or incorrectly signed `raw.mlat` reports.
- Source event IDs, ICAO addresses, finite numbers, bounds, and event times are validated.
- PostgreSQL uses parameterized queries.
- The operator API key protects mutable coverage changes and manual L1 validation.
- Receiver intake uses `X-SkySecure-Receiver-Key` and payload/credential identity matching.
- Compose binds PostgreSQL, Redis, Kafka, API, and frontend host ports to loopback.
- Nginx proxies REST and WebSocket traffic same-origin and supplies CSP, `nosniff`, referrer, permissions, and frame-ancestor protections.
- WebSockets reject unapproved origins.
- Do not expose loopback ports publicly without TLS, authentication, network policy, and an audited reverse proxy.

## Requirements and configuration

### Minimum software

- Docker Engine or Docker Desktop
- Docker Compose v2 (`docker compose`)
- Python 3.9+ for environment preflight and host-side development checks
- `curl` for documented health/API verification commands
- Git for development/update workflows
- Enough disk for PostgreSQL, Redis AOF, Kafka logs, images, and backups

Compose pins Kafka, ZooKeeper, and PostGIS/PostgreSQL images to `linux/amd64`.
Non-amd64 hosts therefore require working amd64 container emulation.

### Environment file

```bash
cp .env.example .env
```

Replace every placeholder. Never print, inspect in shared logs, or commit the resulting `.env`.

| Variable | Purpose | Required by Compose |
|---|---|---|
| `POSTGRES_PASSWORD` | PostgreSQL application password | Yes |
| `MLAT_RECEIVER_API_KEYS` | Exact JSON map of receiver ID to distinct secret | Yes |
| `MLAT_SOLVER_SIGNING_KEY` | Independent solver report signing key | Yes |
| `OPERATOR_API_KEY` | Authorizes mutable operator routes | Recommended; route fails closed when absent |
| `OPENSKY_USERNAME`, `OPENSKY_PASSWORD` | Optional OpenSky credentials | No |
| `ADSB_FALLBACK_LAT/LON` | Default coverage center | No; defaults provided |
| `ADSB_FALLBACK_RADIUS_NM` | Default radius, 1–250 NM | No; default 250 |
| `API_CORS_ORIGINS` | Allowed browser/WebSocket origins when running outside Compose defaults | Environment-dependent |
| `KAFKA_BOOTSTRAP`, `REDIS_URL`, `POSTGRES_DSN` | Host-side development endpoints | Compose sets internal values |
| `LOG_LEVEL` | Logging verbosity | No |

The configured receiver location map currently uses four example European receiver IDs and coordinates. Replace receiver IDs, surveyed coordinates, and credentials together before field use. Credential keys must exactly equal location keys.

## Starting a fresh installation

These instructions are only for a new empty installation.

```bash
cp .env.example .env
# Replace all placeholders in .env.
python3 scripts/validate_env.py .env
docker compose config --quiet
docker compose up -d --build
```

The preflight rejects published placeholders, short required secrets, malformed
receiver-key JSON, fewer than four receiver credentials, and duplicate receiver
credentials. To keep validation identical to the deployed value, the environment
file must use the documented unquoted literal format; inline comments, outer
quotes, and `$` interpolation are rejected. `docker compose config` validates
interpolation and structure; it does not by itself prove that example secrets
were replaced.

Check startup:

```bash
docker compose ps -a
curl -fsS http://127.0.0.1:8000/healthz
docker compose logs --since=5m api fusion-engine anomaly-detector mlat-solver
```

Open:

- Dashboard: `http://127.0.0.1:3000`
- OpenAPI/Swagger: `http://127.0.0.1:8000/docs`
- Health: `http://127.0.0.1:8000/healthz`

A fresh software-only setup may be healthy while `/api/mlat/readiness` returns 503. That is expected without physical receivers.

## Updating an existing installation safely

Do not use unrestricted `docker compose up`, `docker compose down`, or `docker compose down -v` during an application-only production update.

### 1. Back up and record state

Create and verify a PostgreSQL backup outside the repository. Record infrastructure container IDs, restart counts, mounted volumes, topic definitions, consumer offsets, and a live ingestion count.

### 2. Build exactly the six application images

```bash
docker compose build fusion-engine adsb-ingestor api mlat-solver anomaly-detector frontend
```

### 3. Apply the idempotent migration to the running database

```bash
docker compose exec -T postgres \
  psql -U skysecure -d skysecure -v ON_ERROR_STOP=1 \
  < scripts/migrations/001_fusion_event_commits.sql
```

The migration must succeed before application recreation.

### 4. Recreate exactly the six applications, without dependencies

```bash
docker compose up -d --no-deps --force-recreate fusion-engine adsb-ingestor api mlat-solver anomaly-detector frontend
```

Do not include:

- `postgres`
- `redis`
- `kafka`
- `zookeeper`

### 5. Verify the release

Verify health, logs, restart counts, image IDs, source/image hashes where applicable, topic presence, consumer groups, zero or bounded lag, WebSocket upgrade, CSP, outbox pending count, and advancing `track_points`.

## Using the dashboard

The dashboard shows live tracks, risk, classification, layer totals, and trigger evidence.

### Change live coverage

1. Select an airport or pan the map.
2. Choose **Use map center** if needed.
3. Select a 1–250 NM radius.
4. Select **Monitor area**.
5. Supply the operator key when prompted.

Coverage is stored in Redis and shared by the API and ingestor. Updates are lock-protected and rate-limited. `.env` values remain fallback defaults after a fresh Redis deployment.

The browser stores the operator key in `sessionStorage`, not in repository code. Use the same-origin frontend rather than exposing the API directly to arbitrary origins.

## API and WebSocket reference

| Method | Path | Purpose | Authorization |
|---|---|---|---|
| `GET` | `/healthz` | Redis/PostgreSQL/Kafka health | Public on loopback |
| `GET` | `/api/coverage` | Current coverage area | Public on loopback |
| `PUT` | `/api/coverage` | Change coverage | `X-SkySecure-Operator-Key` |
| `GET` | `/api/live-aircraft` | Cached public-feed snapshot with L1 | Public on loopback |
| `GET` | `/api/aircraft?limit=&min_risk=` | Enriched canonical tracks | Public on loopback |
| `GET` | `/api/alerts?limit=&min_score=` | Current alert-worthy tracks | Public on loopback |
| `GET` | `/api/stats` | Current bounded aggregate statistics | Public on loopback |
| `GET` | `/api/layers` | Per-layer evaluated/triggered/skipped counts | Public on loopback |
| `GET` | `/api/layers/{L1-L5}/triggers?limit=` | Current evidence for one layer | Public on loopback |
| `GET` | `/api/l1/sources` | L1 source availability | Public on loopback |
| `POST` | `/api/l1/validate` | Manual claim validation | Operator key |
| `POST` | `/api/mlat/receptions` | Physical receiver event intake | `X-SkySecure-Receiver-Key` |
| `GET` | `/api/mlat/readiness` | Receiver configuration/geometry/liveness readiness | Public on loopback |
| WebSocket | `/ws/tracks` | Same-origin current track snapshots | Origin checked |

Interactive schemas and exact parameters are available at `/docs`.

### WebSocket behavior

The server sends an initial message shaped like:

```json
{
  "type": "snapshot",
  "ts": 1720000000.0,
  "count": 1,
  "aircraft": [],
  "tdoa_enabled": true
}
```

Clients may send `ping` and receive `pong`. Coverage changes and snapshots use the same Redis lock to avoid returning a mixed-area result.

### Receiver intake shape

The payload is a `RawADSBMessage`. A simplified example is:

```json
{
  "receiver_id": "receiver-a",
  "recv_time": 1720000000.123456,
  "icao24": "ABC123",
  "raw_message": "8DABC12358C382D690C8AC2863A7",
  "msg_type": 17
}
```

The actual receiver ID must be configured, the raw message must be 14 or 28 uppercase hexadecimal characters, the event time must be bounded, and the header credential must belong to the same receiver. Do not use example secrets in production.

## Layer 3 model operation

### Current fallback

When no checkpoint is present, `LSTMTrajectoryPredictor` logs that it is using the heuristic predictor. The layer still reports whether trajectory history is warming up, evaluated, or triggered.

### Expected checkpoint

The code expects a PyTorch state dictionary at:

```text
/app/data/trajectory_lstm.pt
```

The expected architecture uses five normalized inputs:

1. latitude
2. longitude
3. barometric altitude
4. ground speed
5. heading

It uses a 20-observation input sequence, a hidden size of 64, and two LSTM layers. Installing an arbitrary state dictionary is not sufficient; its architecture must match exactly and its training/validation provenance must be trusted.

### Safe model deployment checklist

1. Version the dataset, feature transformations, training code, and checkpoint hash.
2. Prevent aircraft/time leakage across train, validation, and test splits.
3. Measure performance on labeled data representative of deployment coverage.
4. Calibrate the anomaly threshold and document confidence intervals.
5. Scan and approve the checkpoint source; load only state dictionaries.
6. Place or mount the artifact at `/app/data/trajectory_lstm.pt`.
7. Rebuild/recreate `anomaly-detector` only after tests and review.
8. Confirm logs say the model loaded and L3 reports `trajectory_lstm` rather than `trajectory_heuristic`.
9. Monitor drift and retain rollback to the prior image/artifact.

The current repository does not ship a scientifically validated production checkpoint.

## Layer 4 physical MLAT operation

### Minimum field prerequisites

- At least four receivers, preferably more.
- Useful geographic separation and nondegenerate geometry.
- Accurately surveyed latitude, longitude, and elevation.
- Clocks disciplined and calibrated to the precision required by TDOA.
- Capture of identical raw Mode-S transmissions with precise event timestamps.
- One distinct identity-bound secret per receiver.
- Secure network transport to the API.
- A separate high-entropy solver signing key.
- Labeled truth/reference tracks for commissioning.

### Configuration steps

1. Replace the example receiver location map consistently in Compose/configuration.
2. Set the exact same receiver IDs in `MLAT_RECEIVER_API_KEYS`.
3. Generate a different secret for every receiver.
4. Generate an independent `MLAT_SOLVER_SIGNING_KEY`.
5. Validate Compose and start the application.
6. Send real receptions and check `/api/mlat/readiness`.
7. Confirm `raw.mlat.receptions` offsets advance.
8. Confirm the solver group consumes receptions and signed `raw.mlat` reports appear.
9. Confirm fusion accepts only valid reports and L4 changes from `SKIPPED` only when event-aligned ADS-B evidence exists.

### Readiness is not accuracy

A `ready` response means enough configured receivers are recently active and geometry/configuration checks pass. It does not prove timing calibration, solve accuracy, RF coverage, or detection performance. Field commissioning must compare results against trusted ground truth.

### Layer 4 quality gates

Reports are rejected for unknown receivers, unsafe geometry, stale/future time, excessive residual, excessive CEP90, invalid provenance count/format, duplicate source IDs, or bad solver signature. Position discrepancies inside the report's CEP90 radius are not labeled contradictions. MLAT altitude is retained as track data but is not used as Layer 4 disagreement evidence because vertical uncertainty and altitude-datum reconciliation are not yet modeled.

## Backup, migration, rollback, and recovery

### PostgreSQL backup example

Choose a destination outside the repository:

```bash
mkdir -p /secure/skysecure-backups
backup="/secure/skysecure-backups/skysecure-$(date -u +%Y%m%dT%H%M%SZ).dump"
docker compose exec -T postgres \
  pg_dump -U skysecure -d skysecure -Fc \
  > "$backup"
docker compose exec -T postgres pg_restore --list < "$backup" >/dev/null
```

Do not treat a zero-byte or unlisted dump as a backup. Verification runs
`pg_restore` inside the version-compatible PostgreSQL container, so a host
PostgreSQL client installation is not required.

### Database migration

`scripts/migrations/001_fusion_event_commits.sql` is idempotent and adds the immutable event ledger/outbox structures required by hardened fusion delivery. Run it against existing PostgreSQL before recreating application services.

### Kafka/ZooKeeper backup

Follow [docs/kafka-persistence.md](docs/kafka-persistence.md). Quiesce applications, verify lag, stop Kafka then ZooKeeper without deleting data, snapshot all broker/coordination volumes as one consistency set, and verify offsets/topics after recovery.

### Rollback

1. Keep the verified database backup and old application image IDs.
2. Do not roll back schema by deleting durable tables while newer events may depend on them.
3. Recreate only the six prior application images with `--no-deps`.
4. Preserve infrastructure containers and volumes.
5. Verify health, offsets, outbox state, and advancing ingestion.

### Disaster recovery order

1. PostgreSQL
2. Redis
3. ZooKeeper
4. Kafka
5. `kafka-init`
6. `fusion-engine` and `mlat-solver`
7. `adsb-ingestor` and `anomaly-detector`
8. API
9. frontend

Verify state and data flow at every dependency boundary rather than trusting container status alone.

## Monitoring and verification

### Basic service checks

```bash
docker compose ps -a
curl -fsS http://127.0.0.1:8000/healthz
curl -fsS http://127.0.0.1:8000/api/layers
curl -i http://127.0.0.1:8000/api/mlat/readiness
docker compose logs --since=5m \
  fusion-engine adsb-ingestor api mlat-solver anomaly-detector frontend
```

A 503 from MLAT readiness is correct when physical receivers are absent.

### Consumer groups

```bash
docker compose exec -T kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 --list

docker compose exec -T kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --describe --group skysecure.anomaly-detector
```

Inspect member count, assigned partitions, current offsets, log-end offsets, and lag. Temporary stale group members can exist until Kafka session timeout after a process restart; persistent duplicate members require investigation.

### Database flow

```bash
docker compose exec -T postgres psql -U skysecure -d skysecure -c \
  "SELECT count(*) AS track_points, max(time) AS newest FROM track_points;"

docker compose exec -T postgres psql -U skysecure -d skysecure -c \
  "SELECT count(*) AS pending FROM fusion_outbox WHERE delivered_at IS NULL;"
```

For a live feed, the newest timestamp and row count should advance. Pending outbox rows may appear transiently; sustained growth indicates delivery failure.

### What to alert on

- `/healthz` 503
- application restart loops
- fatal/traceback log patterns
- Kafka consumer lag growth
- no advancing `track_points`
- growing undelivered fusion or anomaly outboxes
- Redis/PostgreSQL disk pressure
- receiver liveness loss
- MLAT residual/CEP90 rejection spikes after hardware deployment
- L3 model-load failure or unexpected fallback

## Testing and development

The repository currently uses Python `unittest` discovery plus frontend build/security checks.

```bash
# Run in an environment containing requirements.txt dependencies.
python -m unittest discover -v
python -m compileall -q api anomaly processing ingestion models.py

docker compose config --quiet

git diff --check

cd frontend
npm ci
npm run build
npm audit --omit=dev
```

When host dependencies are unavailable, run tests in the current application image with the source mounted, then rebuild affected images and repeat tests inside the rebuilt image before release.

Behavior changes should follow RED-GREEN-REFACTOR:

1. add one focused failing regression;
2. confirm the expected failure;
3. implement the smallest correction;
4. pass the focused test;
5. pass the full suite;
6. rebuild and verify the actual runtime artifact;
7. obtain independent review before commit/push/deploy.

Do not inspect or copy `.env` into test reports or images.

## Troubleshooting

### `/healthz` returns 503

Inspect the response's dependency map, then check Redis, PostgreSQL, and Kafka in that order. A listening port alone does not prove the dependency is usable.

### `/api/mlat/readiness` returns 503

This is expected without real receivers. Otherwise verify:

- credential keys exactly equal receiver-location keys;
- every credential is distinct and nonempty;
- receiver geometry is safe;
- at least the required receiver count has acknowledged recent receptions;
- timestamps are not stale or future-dated.

### Layer 3 is `SKIPPED`

The trajectory window may still be warming up and NIC/NACp may be unavailable. Check `skipped_reason`. Missing integrity metadata is not a trigger.

### Layer 3 uses the heuristic

Check whether `/app/data/trajectory_lstm.pt` exists inside `anomaly-detector`, PyTorch is available, and the state dictionary matches the architecture. Do not claim trained-model operation from file presence alone; confirm the startup log and detector name.

### Tracks appear on the public dashboard but not in canonical analytics

The frontend also polls `/api/live-aircraft` as a public-feed display fallback. Check `raw.adsb` offsets, fusion logs, PostgreSQL rows, `fused.tracks`, anomaly consumer lag, and `sv:{ICAO}` state before declaring the canonical pipeline healthy.

### Consumer starts repeatedly or owns only some partitions

Describe group members and wait at least the configured Kafka session timeout after old processes exit. Confirm only one intended Compose project/container runs the group. Do not reset offsets until the cause is understood.

### Coverage changes fail

- HTTP 403: missing/wrong operator key.
- HTTP 429: wait at least two seconds.
- HTTP 409: coverage changed repeatedly during an external fetch; retry.
- HTTP 503: Redis is unavailable.

### Existing installation tries to attach empty broker volumes

Stop. Do not recreate Kafka/ZooKeeper during an application release. Follow the controlled adoption process in `docs/kafka-persistence.md`, preserve old containers, copy/verify data offline, and use a maintenance window.

## Known limitations

- Public aggregator agreement is not physical TDOA or proof of RF origin.
- Public feed disappearance is not transponder-loss evidence.
- Aggregators can share upstream data and failure modes.
- Raw Mode-S irregularity analysis is unavailable on aggregator-only state vectors.
- Layer 3 heuristic thresholds are not a validated trained model.
- No production LSTM checkpoint ships with the repository.
- Layer 4 software is implemented but operational results require genuine hardware.
- MLAT altitude is not used as Layer 4 disagreement evidence because vertical uncertainty and barometric/geometric datum reconciliation are not yet modeled.
- Receiver timing calibration, multipath/NLOS handling, and field accuracy are not proven.
- Kafka is configured as a single broker with replication factor 1.
- PostgreSQL, Redis, Kafka, and ZooKeeper are single-instance Compose services.
- Host ports are loopback-bound; internet exposure requires additional production security.
- External identity/threat intelligence is not part of the deployed L5 runtime.
- Synthetic/unit tests validate engineering behavior, not real-world accuracy.
- Scientific claims require labeled datasets, documented methodology, and reproducible evaluation.

## Further improvements

All known unimplemented, hardware-dependent, validation-dependent, and scale/security improvements are maintained in:

- [REMAINING_IMPROVEMENTS.md](REMAINING_IMPROVEMENTS.md)

Supporting documents:

- [RUNNING.md](RUNNING.md) — concise production operation procedure
- [INTEGRATION_GUIDE.md](INTEGRATION_GUIDE.md) — current MLAT integration pointer
- [docs/detection-layers.md](docs/detection-layers.md) — telemetry contract
- [docs/kafka-persistence.md](docs/kafka-persistence.md) — broker backup and controlled volume adoption
