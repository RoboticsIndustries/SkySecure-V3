# SkySecure MLAT integration guide

The former simulated-TDOA integration guide is archived because it described obsolete topics, unrestricted full-stack startup, and unvalidated performance claims.

The supported implementation is the authenticated physical-MLAT path documented in:

- `README.md` — architecture, configuration, and migration-first application-only deployment.
- `RUNNING.md` — current canonical L1-L5 runtime behavior and verification.
- `docs/kafka-persistence.md` — Kafka/ZooKeeper backup, recovery, and controlled future volume adoption.

Do not deploy the old simulated `tdoa.validated` / `anomalies.detected` topology. Current physical receptions enter `raw.mlat.receptions`; solver-authenticated reports enter `raw.mlat`; fused events enter `fused.tracks`; anomaly events enter `alerts.anomaly`.

Existing production updates must preserve PostgreSQL, Redis, Kafka, ZooKeeper, and all persistent data. Apply `scripts/migrations/001_fusion_event_commits.sql` to the running PostgreSQL service first, then recreate only the six application services listed in `RUNNING.md`.

SkySecure does not claim measured real-world detection accuracy, false-positive rates, production LSTM performance, or physical-MLAT effectiveness without genuine authenticated receiver traffic and labeled ground truth.
