# SkySecure detection layers

SkySecure uses one canonical layer taxonomy across event payloads, APIs, logs, and the dashboard.

| Layer | Purpose | Current implementation |
|---|---|---|
| L1 | Position and source validation | Compares public ADS-B aggregators that may share upstream data. This is not physical receiver TDOA. |
| L2 | Kinematic and behavioral detection | Deterministic physics checks, timestamp-aware acceleration and turn rate, persistent per-aircraft statistical baselines, and passive aircraft-pair conflict projection. |
| L3 | Learned trajectory and navigation integrity | Sequence trajectory detector plus NIC/NACp evidence. Uses a heuristic fallback until a trained LSTM checkpoint is installed. |
| L4 | Multi-sensor fusion | Compares event-time-aligned ADS-B and authenticated MLAT positions when physical data is available; horizontal thresholds respect reported CEP90. |
| L5 | Identity and threat intelligence | Duplicate ICAO detection today; external intelligence and airspace policy are future work. |

## Trigger contract

Every canonical anomaly flag produced by the detector pipeline includes:

- `layer`: canonical layer (`L1` through `L5`)
- `detector`: stable detector identifier
- `type`: anomaly category
- `score_delta`: contribution to the current risk score
- `description`: concise human-readable explanation
- `evidence`: measured values and thresholds used by the detector
- `timestamp`: event time

Raw/public-feed fallback records may expose simplified display anomalies with
only `type` and `description`; they are not canonical detector flags.

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

L2 baselines are stored in Redis under `baseline:l2:{icao24}` and survive detector restarts. Current timestamp-aware checks include acceleration and turn rate; baseline checks cover velocity and vertical rate. Operational public-feed checks cover impossible speed, altitude jumps, and teleportation. Barometric/GNSS altitude-disagreement code exists, but geometric altitude is not populated by the shipped public-feed adapters, so that comparator is not operational on the current public-feed path. Transponder-loss alerts are enabled only for receiver-backed reports; a disappearance from OpenSky or adsb.lol is treated as an aggregator/coverage dropout, not proof that the transponder was switched off.

L2 is software-complete for test and field calibration. Production acceptance still requires labeled real-world replay data, measured false-positive/detection rates, and threshold calibration. Raw Mode-S integrity checks remain blocked until ingestion provides raw Mode-S messages instead of aggregator-only state vectors.

### TCAS-inspired conflict analytics

L2 also performs TCAS-inspired conflict analytics through a dedicated `conflict-monitor` service. The monitor reads only canonical `fusion:sv:*` records, selects one coherent ADS-B `SourceReport` per aircraft, and publishes a bounded shared snapshot at `conflict:latest`. The API and dashboard consume that snapshot; they do not calculate conflict state. This is SkySecure-generated relative-motion evidence, not an onboard TCAS/ACAS Traffic Advisory or Resolution Advisory. It never generates climb or descend commands and is not certified collision-avoidance guidance.

A track is eligible only when its newest complete ADS-B source report contains a valid six-hex ICAO identity, finite position, barometric altitude, speed, heading, vertical rate, airborne state, and a source-event age of at most 15 seconds. A newer partial report cannot refresh retained kinematics. Candidate pairs more than five seconds apart are skipped. Eligible observations are propagated to the newer source epoch in one Earth-centered position/velocity frame, which avoids dateline and polar frame errors.

The conservative engineering thresholds are deliberately not presented as TCAS thresholds:

- `MONITOR`: simultaneous horizontal separation at most 10 NM and vertical separation at most 2,000 ft.
- `TRAFFIC_CONFLICT`: simultaneous horizontal separation at most 5 NM and vertical separation at most 1,000 ft.
- `PREDICTED_LOSS_OF_SEPARATION`: simultaneous horizontal separation at most 3 NM and vertical separation at most 1,000 ft.
- Projection horizon: 120 seconds.

The detector solves simultaneous horizontal and vertical violation intervals over the complete prediction horizon. CPA/TCPA remain diagnostic outputs; vertical separation only at horizontal CPA is not used as the event condition.

Candidate generation uses bounded Earth-centered spatial buckets with a conservative 100 NM envelope, a 100,000-pair evaluation budget, and a 500-conflict output budget. Exceeding the pair budget makes the entire cycle unavailable rather than publishing an order-dependent partial result. World-scan history gathered at different times is excluded.

Lifecycle state is bounded and retained in Redis. Non-immediate projections require two consecutive observations before activation; entries within 20 seconds may activate immediately. Active pairs require three consecutive clear cycles before removal. Provider outages or unavailable cycles retain active pairs as `STALE_HOLD`/`INSUFFICIENT_DATA` and do not count as clears. The rendered snapshot expires after 30 seconds and lifecycle state after 120 seconds.

The stable detector name is `aircraft_conflict_projection`. Results appear separately from per-aircraft detector totals under L2 relational telemetry, at `GET /api/conflicts`, and in each WebSocket snapshot's `conflict_analysis` field. Genuine live ACAS advisory detection remains unavailable without authorized raw Mode-S/ACAS evidence.
