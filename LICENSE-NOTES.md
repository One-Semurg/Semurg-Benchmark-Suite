# Licensing posture of the arena kit

PUBLIC BOARD (shipped, runnable, publishable): only engines whose licences permit third-party
benchmarking and self-hosting -- SQLite, DuckDB, PostgreSQL, MySQL, MariaDB, ClickHouse, TimescaleDB
(Apache-2 core), QuestDB, MongoDB (self-host), Redis (BSD/RSAL self-host), Neo4j CE, Kuzu, Faiss,
Qdrant, OpenSearch, RocksDB, Kafka, MinIO.

OPT-IN, LOCAL-ONLY (never shipped, never published by Semurg): kdb+/DeWitt clause, Elasticsearch
(ELv2), TigerGraph (EULA), Memgraph (BSL). You install these from the vendor under your own licence;
the kit runs the identical workload locally and keeps results on your machine only.

The Semurg R11 release in installer/ is Semurg's own software, distributed per its bundled licence.
