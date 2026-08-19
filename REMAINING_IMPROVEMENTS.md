# SkySecure V3 — Remaining Improvements

This document lists work that is not fully implemented, not connected to the deployed runtime, dependent on hardware, or not yet scientifically validated. It is intentionally separate from the main README so current behavior and future ambition are not confused.

Nothing here should be represented as operational until its acceptance criteria are met with reproducible evidence.

## Priority legend

- **P0 — trust/durability:** must be resolved before trusting a new evidence source or scaling production.
- **P1 — operational capability:** high-value work needed for field usefulness.
- **P2 — quality/scale:** improves accuracy, resilience, observability, or maintainability.
- **P3 — research/product:** longer-term expansion.

## Current honest baseline

Implemented now:

- Public-feed ingestion and L1 cross-source comparison.
- Canonical event-time-aware fusion.
- L2 deterministic/statistical checks.
- L3 NIC/NACp integrity and heuristic trajectory fallback.
- Fail-closed physical receiver intake, MLAT solving, signed provenance, and L4 comparison software.
- L4 CEP90-aware horizontal disagreement checks.
- L5 duplicate-ICAO evidence.
- PostgreSQL event ledger/outbox, Redis live state, exact Kafka offset commits, API, WebSocket, and dashboard.

Not proven now:

- Physical MLAT field performance.
- Production LSTM performance.
- Real-world detection/false-positive rates.
- Certified surveillance suitability.
- High-availability infrastructure.

## P0 — preserve trust and durability

### Renew long-running fusion outbox leases

Current fusion delivery uses a 30-second PostgreSQL lease and owner-conditional completion. Most broker acknowledgements should be far shorter, but the lease is not renewed while an unusually slow acknowledgement is pending.

Improve by:

- adding owner-conditional lease renewal while publishing;
- fencing delivery completion if ownership is lost;
- proving cancellation and process-crash behavior;
- retaining at-least-once semantics.

Acceptance criteria:

- deterministic tests where broker acknowledgement exceeds one lease period;
- no two workers can both mark the same envelope delivered;
- lease loss cannot delete or mutate another worker's claim;
- retry remains safe after worker termination.

### End-to-end secret management

Move long-lived credentials from a local `.env` file to an approved secret manager or Docker secret mechanism.

Acceptance criteria:

- no secrets in Git, image layers, logs, process arguments, or browser payloads;
- documented rotation for receiver keys, solver signing key, operator key, and database password;
- overlap/rollover strategy that does not create an unauthenticated window;
- auditable access policy.

### TLS and authenticated network boundaries

Loopback binding is safe for a single host but not a complete distributed deployment design.

Acceptance criteria:

- TLS from receivers to ingress and from external clients to frontend/API;
- mutual TLS or equivalent device identity for receivers where practical;
- firewall/network policy between application, broker, cache, and database tiers;
- authenticated Kafka/Redis/PostgreSQL transport when moved off-host;
- tested certificate rotation and failure modes.

### Receiver key rotation and revocation

Add explicit key versioning/revocation instead of replacing the whole key map at once.

Acceptance criteria:

- one compromised receiver can be revoked independently;
- old and new credentials can overlap for a bounded rotation window;
- replayed old credentials fail after revocation;
- readiness reports revoked/untrusted receivers separately.

## P1 — Layer 1 improvements

### Use feeds with documented independence

Public aggregators may share upstream receivers. Add commercial or receiver-provenance-aware feeds.

Acceptance criteria:

- every claim includes stable source and provenance metadata;
- independence assumptions are documented and tested;
- one provider's rebroadcast cannot corroborate itself;
- rate limits, licensing, and retention terms are respected.

### Persist and replay L1 evidence canonically

L1 currently operates mainly in the API live snapshot path.

Improve by:

- defining an immutable L1 claim/result event schema;
- persisting raw source claims and validation output;
- joining L1 evidence into canonical track history without blocking ingestion;
- making API summary restart-stable.

Acceptance criteria:

- replay gives the same L1 verdict;
- source-specific event time is preserved;
- delayed claims cannot overwrite newer source evidence;
- the same aggregator cannot count twice through aliases.

### Calibrate cross-source thresholds

Acceptance criteria:

- labeled clean/spoofed/uncertain scenarios;
- error distribution by region, altitude, source pair, and latency;
- thresholds chosen from documented objectives;
- uncertainty shown to operators rather than reduced to unsupported certainty.

## P1 — Layer 2 improvements

### Aircraft-type-aware kinematic envelopes

A fighter, airliner, helicopter, balloon, and ground vehicle should not share every threshold.

Acceptance criteria:

- trustworthy aircraft type lookup with provenance;
- safe fallback when type is unknown;
- per-class acceleration, turn, climb, altitude, and speed envelopes;
- tests preventing identity changes from silently changing detector state.

### Raw Mode-S integrity checks

Aggregator state vectors do not supply enough raw-message context.

Potential checks:

- malformed or impossible DF/field combinations;
- parity/CRC and address recovery behavior;
- capability/version inconsistencies;
- improbable message-rate or field-transition patterns;
- receiver-specific RF/decoding quality.

Acceptance criteria:

- direct receiver input with exact raw frames and provenance;
- decoder conformance tests;
- malformed inputs fail safely without poisoning the stream;
- no claim that an aggregator dropout is transmitter loss.

### Context-aware behavior

Add weather, flight phase, airport/runway proximity, and airspace context only from licensed and timestamped sources.

Acceptance criteria:

- context source/version/event time recorded in evidence;
- stale context is unavailable, not assumed current;
- detectors remain deterministic under replay.

## P1 — Layer 3 model program

### Build a reproducible training dataset

Needed data:

- clean trajectories from diverse aircraft and regions;
- labeled anomalies or defensible synthetic transformations kept separate from evaluation truth;
- NIC/NACp and source-quality metadata, plus RAIM only where a trusted adapter defines and validates its semantics;
- aircraft type, phase of flight, coverage, and sampling intervals where trustworthy.

Acceptance criteria:

- versioned dataset manifest and hashes;
- aircraft/time/geography leakage controls;
- documented exclusions and label quality;
- legally permitted storage and use.

### Train and validate the LSTM or a better sequence model

The code expects a two-layer PyTorch LSTM state dictionary, but architecture should be selected by evidence rather than preserved by inertia.

Acceptance criteria:

- reproducible training configuration and random seeds;
- held-out test set never used for threshold selection;
- comparison against the heuristic and simple statistical baselines;
- per-scenario precision/recall, ROC/PR curves, calibration, and confidence intervals;
- latency/CPU/memory measurements on deployment hardware;
- versioned checkpoint hash and model card.

### Make trajectory prediction time-aware

The current fallback operates on observation order and simple position extrapolation. Improve features to include elapsed event time, longitude scaling by latitude, circular heading representation, missingness masks, and source quality.

Acceptance criteria:

- irregular sampling tests;
- dateline/pole and heading-wrap tests;
- delayed/replayed events do not advance sequence state;
- error measured in physical units with calibrated uncertainty.

### Model registry and safe rollout

Acceptance criteria:

- model ID/hash visible in health/layer telemetry;
- startup fails or explicitly falls back according to policy;
- canary comparison without double-counting risk;
- drift and missing-feature monitoring;
- one-command rollback to approved prior model.

## P1 — physical Layer 4 commissioning

### Build receiver hardware

Minimum design questions:

- SDR/Mode-S receiver and timestamping hardware;
- disciplined clock source and holdover behavior;
- surveyed antenna location/elevation;
- timestamp precision and transport format;
- local buffering during network loss;
- tamper-resistant device identity and updates.

Acceptance criteria:

- at least four deployed receivers with safe geometry;
- same transmission observed with measured timestamp precision;
- calibration procedure and uncertainty budget;
- secure authenticated transport;
- health telemetry for clock lock, packet loss, RF level, and local queue depth.

### Field-calibrate clocks and receiver delays

Account for antenna, cable, SDR, firmware, clock, and processing delays.

Acceptance criteria:

- per-receiver delay estimates with version/time validity;
- reference emitter or trusted target validation;
- automatic detection of clock step/drift/holdover;
- unhealthy receivers excluded without making readiness lie.

### Model vertical uncertainty explicitly

Current reports expose horizontal CEP90 but do not model vertical uncertainty or reconcile geometric and barometric altitude datums. Altitude is therefore retained as track data but is not used as L4 disagreement evidence.

Acceptance criteria:

- vertical error/covariance from solver geometry;
- separate horizontal and vertical acceptance gates;
- altitude evidence threshold derived from uncertainty, not a fixed 3,000-ft constant;
- barometric/geometric altitude datum conversion documented and tested.

### Improve solver robustness

Potential work:

- robust loss/outlier rejection;
- receiver clock-bias estimation;
- multipath/NLOS detection;
- geometry dilution metrics;
- adaptive weighting from timestamp quality;
- multi-start solve and convergence diagnostics.

Acceptance criteria:

- adversarial/outlier tests;
- no unsafe solve marked valid;
- uncertainty calibration against ground truth;
- bounded CPU/memory and accumulator state.

### Receiver/network readiness detail

Expand readiness beyond recent activity.

Acceptance criteria:

- clock lock/quality;
- surveyed-coordinate version;
- firmware/protocol version;
- recent valid/invalid reception counts;
- geometry quality for currently active subset;
- Kafka acknowledgement and solver output freshness;
- reason codes consumable by monitoring.

## P1 — Layer 5 expansion

### Authoritative identity registry

Acceptance criteria:

- licensed, timestamped, updateable registry source;
- provenance on registration/operator/type claims;
- conflict handling and stale-record policy;
- identity changes do not overwrite historical truth.

### External threat intelligence

Potential inputs include sanctions, watch lists, government/defense notices, and operator-maintained cases. These are sensitive and can create legal/safety risk.

Acceptance criteria:

- authorized data source and retention policy;
- role-based access control and audit log;
- explicit source/confidence/time on every match;
- human review and correction workflow;
- no unsupported inference from nationality or military ICAO range alone.

### Airspace and mission policy

Acceptance criteria:

- versioned geofences and effective times;
- altitude/time dimensions;
- NOTAM/temporary restriction provenance where licensed;
- replay-deterministic policy evaluation;
- clear separation between safety alert, security anomaly, and policy violation.

### Connect or retire dormant classifier code

The repository contains military classification/formation logic that is not a deployed Compose service.

Acceptance criteria for connection:

- explicit event input/output contract;
- bounded state and scans;
- source/provenance ownership;
- tests and runtime service definition;
- no duplicate scoring with L2/L5;
- honest API/dashboard exposure.

Otherwise, remove or archive it to reduce architectural ambiguity.

## P2 — observability and operations

### Metrics and tracing

Add Prometheus/OpenTelemetry instrumentation for:

- input/output rates by topic/partition;
- consumer lag;
- provider fetch latency/errors/rate limits;
- fusion/outbox delivery latency and retries;
- Redis operation latency and scan cycle age;
- detector evaluated/triggered/skipped counts;
- receiver activity, solve rate, residual, CEP90, and rejection reason;
- API latency/status and WebSocket clients;
- database insert latency and storage growth.

Acceptance criteria:

- dashboards and actionable alerts;
- bounded metric cardinality;
- trace/event IDs connect source intake to output;
- no secrets or raw sensitive payloads in telemetry.

### Dead-letter/quarantine workflow

Poison records should be inspectable without blocking partitions or disappearing silently.

Acceptance criteria:

- immutable quarantine envelope with source topic/partition/offset and validation reason;
- access controls and retention;
- deterministic repair/replay tooling;
- original consumer commit behavior remains explicit and safe.

### Operational runbooks

Add tested runbooks for:

- Kafka/Redis/PostgreSQL outage;
- receiver compromise;
- solver-signing-key rotation;
- outbox backlog;
- consumer lag/reset decision;
- bad model rollback;
- disk pressure;
- region/provider outage;
- disaster recovery exercise.

## P2 — scale and high availability

### Kafka high availability

Current Compose uses one broker and replication factor 1.

Acceptance criteria:

- at least three brokers for production where required;
- replication/min-ISR policy;
- tested broker loss and rolling upgrade;
- partition capacity plan;
- authenticated/encrypted listeners;
- migration plan from ZooKeeper to KRaft if adopted.

### PostgreSQL resilience

Acceptance criteria:

- continuous backup/PITR;
- replica/failover strategy;
- tested restore with recovery-time/recovery-point objectives;
- schema migration change control;
- retention/partitioning plan for `track_points`.

### Redis resilience

Acceptance criteria:

- persistence and recovery objectives;
- replica/sentinel or managed service where needed;
- memory sizing/eviction policy that protects required keys;
- tested failover without stale cross-owner writes.

### Horizontal application scaling

Acceptance criteria:

- consumer partition assignment understood per service;
- no process-local state required for correctness, or state is partition-affine/durable;
- per-aircraft serialization works across replicas;
- API caches and WebSocket broadcast remain coherent;
- load tests cover maximum expected tracks and clients.

## P2 — API, frontend, and operator workflow

### Authentication and RBAC

Current mutable routes use one operator key.

Acceptance criteria:

- authenticated users/service accounts;
- roles for viewer, operator, receiver admin, investigator, and system admin;
- audit log for configuration and evidence actions;
- short-lived credentials and revocation;
- CSRF/session policy if cookies are used.

### Investigation workflow

Potential features:

- alert acknowledgement and disposition;
- evidence timeline by aircraft/event;
- source report comparison;
- false-positive labeling;
- export with hashes/provenance;
- analyst notes and case linkage.

Acceptance criteria:

- immutable original evidence;
- audited edits;
- access and retention policy;
- analyst labels can feed evaluation without contaminating test data.

### Historical replay UI

Acceptance criteria:

- event-time playback from PostgreSQL/Kafka archives;
- deterministic layer output for a pinned software/model/config version;
- clear separation from live mode;
- no replay event can enter live alerting accidentally.

## P2 — performance and maintainability

### Consolidate duplicated live/public and canonical paths

The dashboard can show public-feed fallback data while canonical analytics flow through Kafka/fusion/anomaly state.

Acceptance criteria:

- one documented source-of-truth policy;
- UI clearly marks fallback versus canonical records;
- fallback does not bypass persistence/analytics unnoticed;
- shared normalization utilities reduce parser drift.

### Typed internal event schemas and compatibility policy

Acceptance criteria:

- explicit schema version on Kafka events;
- backward/forward compatibility tests;
- migration policy for persisted Redis/Kafka payloads;
- consumers reject unsupported versions safely.

### Configuration cleanup

Move remaining detector thresholds into a validated configuration schema once `config.py` encoding/format is normalized.

Acceptance criteria:

- finite/ranged values;
- Compose and `.env.example` alignment;
- startup validation;
- threshold version included in evidence/replay metadata.

### Dependency and image hardening

Acceptance criteria:

- pinned dependency versions/hashes;
- regular vulnerability scanning and SBOM;
- non-root runtime users where feasible;
- minimal images;
- signed images and provenance;
- automated update testing.

## P1/P2 — scientific validation program

No detector should receive a performance claim without this program.

### Dataset requirements

- representative airspace, aircraft types, weather, coverage, and sampling rates;
- trustworthy ground truth;
- known clean and known anomalous cases;
- separation of naturally occurring unusual behavior from malicious behavior;
- documented missingness and provider bias.

### Evaluation requirements

- predeclared metrics and thresholds;
- confidence intervals;
- per-layer and combined performance;
- false alarms per track-hour or another operationally meaningful denominator;
- scenario breakdown, not only aggregate score;
- ablation against simple baselines;
- reproducible software/model/config versions;
- independent review.

### Field acceptance

- shadow mode before alerts influence decisions;
- analyst review and feedback;
- threshold change control;
- drift monitoring;
- rollback criteria;
- explicit statement that SkySecure is decision support, not certified separation/surveillance equipment.

## P3 — research directions

These are exploratory, not commitments:

- RF fingerprinting from controlled direct-receiver IQ/features;
- multilateration using mixed receiver quality and online clock-bias estimation;
- graph-based formation/group behavior;
- cross-region identity continuity;
- satellite/space-based ADS-B sources with provenance;
- ACARS correlation where lawful and technically reliable;
- privacy-preserving multi-operator receiver federation;
- explainable sequence models with uncertainty;
- active learning from analyst dispositions;
- simulation/digital-twin scenarios clearly labeled as synthetic.

## Recommended execution order

1. Preserve trust: lease renewal, secrets, TLS, key rotation.
2. Build observability before adding data sources or hardware.
3. Deploy and calibrate physical receivers in shadow mode.
4. Establish labeled evaluation and ground truth.
5. Improve Layer 4 uncertainty/solver behavior from field evidence.
6. Build the Layer 3 reproducible model program.
7. Add authoritative L5 data with governance and RBAC.
8. Scale Kafka/PostgreSQL/Redis after measured capacity requires it.
9. Add analyst workflow and historical replay.
10. Publish performance claims only after independent reproducible validation.

## Definition of done for any roadmap item

An item is not complete merely because code exists. It must have:

- a clear runtime owner and connected entrypoint;
- validated configuration and fail-closed behavior;
- focused RED-GREEN regressions;
- full-suite and built-image verification;
- persistence/replay semantics where stateful;
- bounded CPU, memory, I/O, and external calls;
- operator documentation and rollback;
- security review;
- independent exact-tree code review;
- runtime proof in the intended environment;
- no unsupported scientific or operational claims.
