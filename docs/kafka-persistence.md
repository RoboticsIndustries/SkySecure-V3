# Kafka and ZooKeeper persistence operations

SkySecure mounts Kafka broker logs at `/var/lib/kafka/data` (`kafkadata`) and ZooKeeper snapshots/transaction logs at `/var/lib/zookeeper/data` and `/var/lib/zookeeper/log` (`zookeeperdata`, `zookeeperlog`).

## Backup

1. Quiesce application producers and consumers.
2. Confirm consumer lag is zero and record topic/partition configuration.
3. Stop Kafka, then ZooKeeper, without deleting containers or volumes.
4. Snapshot all three named volumes as one consistency set.
5. Restart ZooKeeper, Kafka, `kafka-init`, then application services.
6. Verify topic metadata, committed consumer offsets, and advancing ingestion.

Never use `docker compose down -v` for backup, upgrade, or rollback.

## Upgrade or first adoption on an existing ephemeral deployment

Treat the change as controlled infrastructure maintenance, not an application deployment. Preserve the existing containers until their broker data is copied into the named volumes and verified offline. Recreate only ZooKeeper and Kafka during an approved maintenance window, one dependency at a time, then verify topic metadata and consumer-group offsets before allowing producers to resume. Keep the stopped original containers until ingestion and replay checks pass.

The normal application release procedure must not recreate PostgreSQL, Redis, Kafka, or ZooKeeper. Therefore adding these mounts to Compose does not authorize an automatic live broker migration.

## Recovery verification

After restore or upgrade, verify:

- all required topics and partition counts;
- consumer groups and committed offsets;
- no unexpected `auto_offset_reset=latest` jump;
- producer acknowledgements;
- advancing PostgreSQL `track_points`;
- application logs and restart counts.
