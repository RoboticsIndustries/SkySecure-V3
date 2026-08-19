# Running SkySecure v3

SkySecure is a real-time ADS-B security research platform. The deployed pipeline currently wires:

- L1: independent public-feed position/source cross-validation.
- L2: kinematic rules and statistical baselines.
- L3: trajectory checks; it uses a documented heuristic fallback when `/app/data/trajectory_lstm.pt` is absent.
- L4: authenticated physical-receiver MLAT comparison. It reports unavailable/skipped until genuine, signed, event-aligned physical receptions exist.
- L5: identity/threat evidence actually supplied by upstream processing; no unsupported external intelligence is inferred.

No measured detection-accuracy or false-positive claim is made without labeled ground truth.

## Fresh development installation

A new, empty development environment may initialize the complete stack:

```bash
docker compose up -d
```

This is not the production update procedure for an existing installation.

## Existing production update

Never recreate PostgreSQL, Redis, Kafka, or ZooKeeper during an application release. Preserve their container identities, storage, and data.

1. Back up PostgreSQL and record infrastructure container IDs/restart counts.
2. Apply the idempotent migration to the running PostgreSQL container:

```bash
docker compose exec -T postgres psql -U skysecure -d skysecure -v ON_ERROR_STOP=1 < scripts/migrations/001_fusion_event_commits.sql
```

3. Build and recreate only application services:

```bash
docker compose build fusion-engine adsb-ingestor api mlat-solver anomaly-detector frontend
docker compose up -d --no-deps --force-recreate fusion-engine adsb-ingestor api mlat-solver anomaly-detector frontend
```

4. Verify `/healthz`, `/api/mlat/readiness`, `/api/layers`, same-origin REST/WebSocket behavior, CSP, logs, restart counts, deployed image IDs, and advancing `track_points`.

The newly declared Kafka/ZooKeeper named volumes are for fresh installations and a future controlled adoption window. Do not recreate the current broker stack merely to adopt them. See `docs/kafka-persistence.md`.

## Physical MLAT trust path

1. Each configured receiver submits a bounded-time, valid Mode-S reception to the API with its identity-bound HMAC credential.
2. The API publishes acknowledged receptions to `raw.mlat.receptions`.
3. The solver requires a complete, nonempty, distinct receiver credential map and safe receiver geometry.
4. A solved report carries one distinct canonical source-event ID per receiver and an HMAC-SHA256 solver signature.
5. Fusion verifies that signature before trusting `raw.mlat`, persists the immutable event and fused payload in PostgreSQL, then publishes through the transactional outbox.
6. Kafka offsets are committed only for the completed record's partition at `offset + 1`.

Without authenticated physical evidence, L4 must remain unavailable rather than fabricating validation.

## Health checks

```bash
curl -fsS http://localhost:8000/healthz
curl -fsS http://localhost:8000/api/mlat/readiness
curl -fsS http://localhost:8000/api/layers
```

MLAT readiness may correctly return HTTP 503 until enough configured physical receivers are recently active. Layer 3 may correctly report heuristic fallback when the trained checkpoint is absent.
