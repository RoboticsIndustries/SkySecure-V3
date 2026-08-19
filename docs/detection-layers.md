# SkySecure detection layers

SkySecure uses one canonical layer taxonomy across event payloads, APIs, logs, and the dashboard.

| Layer | Purpose | Current implementation |
|---|---|---|
| L1 | Position and source validation | Compares public ADS-B aggregators that may share upstream data. This is not physical receiver TDOA. |
| L2 | Kinematic and behavioral detection | Deterministic physics checks, timestamp-aware acceleration and turn rate, and persistent per-aircraft statistical baselines. |
| L3 | Learned trajectory and navigation integrity | Sequence trajectory detector plus NIC/NACp evidence. Uses a heuristic fallback until a trained LSTM checkpoint is installed. |
| L4 | Multi-sensor fusion | Compares event-time-aligned ADS-B and authenticated MLAT positions when physical data is available; horizontal thresholds respect reported CEP90. |
| L5 | Identity and threat intelligence | Duplicate ICAO detection today; external intelligence and airspace policy are future work. |

## Trigger contract

Every anomaly includes:

- `layer`: canonical layer (`L1` through `L5`)
- `detector`: stable detector identifier
- `type`: anomaly category
- `score_delta`: contribution to the current risk score
- `description`: concise human-readable explanation
- `evidence`: measured values and thresholds used by the detector
- `timestamp`: event time

Timestamps come from source events rather than worker processing time. ADS-B
and MLAT measurements must be within five seconds to produce an L4 comparison.
L4 trigger evidence expires after 60 seconds unless a newer MLAT evaluation
supersedes it.

ADS-B and MLAT updates for the same ICAO address are serialized through the
complete Redis read/modify/write and Kafka publish operation. Fusion owns
`fusion:sv:{icao24}` while the anomaly service owns enriched `sv:{icao24}`
records, preventing cross-service stale writes. Consumers use manual offset
commits only after required side effects receive broker acknowledgement,
providing at-least-once retry behavior rather than silently losing transient
failures.

Each state vector also contains `layer_evaluations`. An evaluation status is:

- `EVALUATED`: eligible detectors ran and did not trigger
- `TRIGGERED`: one or more eligible detectors triggered
- `SKIPPED`: the layer could not evaluate the track; `skipped_reason` explains why

## Runtime visibility

- `GET /api/layers` returns evaluated, skipped, triggered, and trigger counts for all layers.
- `GET /api/layers/{layer}/triggers` returns current trigger evidence for one layer.
- The EDA dashboard displays layer totals and lets operators inspect trigger evidence.

## L2 operational scope

L2 baselines are stored in Redis under `baseline:l2:{icao24}` and survive detector restarts. Current timestamp-aware checks include acceleration and turn rate; baseline checks cover velocity and vertical rate. Existing deterministic checks cover impossible speed, altitude jumps, barometric/GNSS altitude disagreement, and teleportation. Transponder-loss alerts are enabled only for receiver-backed reports; a disappearance from OpenSky or adsb.lol is treated as an aggregator/coverage dropout, not proof that a transponder was switched off.

L2 is software-complete for test and field calibration. Production acceptance still requires labeled real-world replay data, measured false-positive/detection rates, and threshold calibration. Raw Mode-S integrity checks remain blocked until ingestion provides raw Mode-S messages instead of aggregator-only state vectors.
