#!/usr/bin/env bash
set -euo pipefail

for spec in \
  raw.adsb:8 \
  raw.mlat.receptions:8 \
  raw.mlat:4 \
  raw.acars:4 \
  fused.tracks:8 \
  alerts.anomaly:4
do
  topic=${spec%%:*}
  partitions=${spec##*:}
  kafka-topics \
    --bootstrap-server kafka:29092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1
done

echo "Kafka topics ready."
