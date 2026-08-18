# SkySecure V3

Real-time ADS-B security research platform for canonical L1-L5 detection telemetry, replay, fusion, and operator visibility.

> Research status: the current public-feed deployment is an engineering prototype. It does not claim scientifically validated detection or false-positive rates, physical receiver TDOA, a trained production LSTM, or raw Mode-S irregularity detection.

## Detection layers

| Layer | Purpose | Current implementation |
|---|---|---|
| L1 | Position and source validation | Cross-validates independent public ADS-B aggregators. Aggregator agreement is not physical receiver TDOA. |
| L2 | Kinematic and behavioral detection | Timestamp-aware acceleration, turn rate, vertical rate, ADS-B position conflict, and Redis-backed statistical baselines. |
| L3 | Trajectory fingerprinting | Explicitly reports either a loaded trained model or the current heuristic fallback, with warm-up `SKIPPED` telemetry. |
| L4 | Multi-sensor fusion | Event-time-aligned ADS-B/MLAT comparison when measurements fall within the configured window. ADS-B-only tracks are `SKIPPED`. |
| L5 | Identity and threat intelligence | Duplicate-ICAO and identity evidence with non-ratcheting active-evidence scoring. |

See `docs/detection-layers.md` for the telemetry contract, lifecycle semantics, and limitations.

## Architecture

```text
public ADS-B feeds ──> adsb-ingestor ──> Kafka raw.adsb
                                          ├─> fusion-engine ──> Kafka fused.tracks
physical receivers* ─> mlat-solver ───────┘                       │
                                                                  v
                                                        anomaly-detector
                                                                  │
                                                     Redis/PostgreSQL/Kafka
                                                                  │
                                                      FastAPI + dashboard
```

`*` Physical synchronized receiver inputs are not part of the current public-feed deployment.

Core services:

- Kafka and ZooKeeper for event transport.
- Redis for live enriched state, fusion-owned source state, MLAT accumulator state, and L2 baselines.
- PostgreSQL/PostGIS for track persistence.
- FastAPI for health, aircraft, layer-summary, trigger, alert, and WebSocket APIs.
- A static dashboard for map and L1-L5 telemetry views.

## Quick start

Prerequisites:

- Docker with Compose support.
- An `.env` based on `.env.example`.

```bash
cp .env.example .env
# Fill in the required local values without committing secrets.
docker compose up -d --build
```

Open:

- Dashboard: `http://localhost:3000`
- API docs: `http://localhost:8000/docs`
- Health: `http://localhost:8000/healthz`

### Change live coverage

The dashboard's **Live Coverage** row can switch the running feed without a restart:

- Choose one of the listed airports, then select **Monitor area**.
- Or pan the map, select **Use map center**, choose a radius, and select **Monitor area**.
- Radius is limited to 1–250 nautical miles by the current public point-feed provider.

The selection is stored in Redis and is shared immediately by the API and ADS-B ingestor. Environment values in `.env` remain the fallback defaults after a fresh Redis deployment.

The mutable API is intentionally bound to loopback by Compose and rate-limited. Put authentication in front of the API before exposing it beyond the host.

Check the stack:

```bash
docker compose ps -a
docker compose logs --since=5m api anomaly-detector fusion-engine mlat-solver
```

## Verification

The repository test environment must contain the dependencies in `requirements.txt` plus `pytest`.

```bash
python -m pytest -q
python -m compileall -q api anomaly processing ingestion config.py coverage_area.py models.py
cd frontend && npm run build
cd .. && git diff --check
docker compose config --quiet
```

Deterministic replay and integration tests cover canonical telemetry, timestamp-aware L2 behavior, event-aligned L4 comparisons, stale-evidence lifecycle, upstream evidence preservation, persistent baselines, scoring stability, Kafka delivery ordering, and MLAT accumulator restoration.

## Important limitations

- Public aggregator cross-validation is not physical TDOA.
- Aggregator feed disappearance is not evidence that an aircraft transponder stopped transmitting.
- Physical transponder-loss evidence requires identified direct-receiver provenance.
- L3 is a heuristic unless a trained and validated model is explicitly loaded.
- Aggregated feeds do not currently supply raw Mode-S frames for raw-message irregularity checks.
- Synthetic replay validates engineering behavior, not real-world accuracy.
- Scientific performance claims require labeled real-world datasets and documented methodology.

## License

MIT License — see `LICENSE`.
