# flink-connector-qdrant

An Apache Flink Table/SQL **sink** connector for [Qdrant](https://qdrant.tech) — write a stream
(e.g. embeddings produced by an upstream Flink job) straight into a Qdrant collection via SQL,
no custom `DataStream` job needed.

Targets Flink **1.20 (LTS)**. Modeled structurally after
[itinycheng/flink-connector-clickhouse](https://github.com/itinycheng/flink-connector-clickhouse)
(dynamic table factory + sink + row converter + connection provider shape) — same reason that one
exists: there's no official Flink connector for this store, and community gaps like this one
(ClickHouse's SQL support was the same story) are worth filling properly instead of hand-rolling a
one-off `DataStream` job each time the need comes up.

## Status

v1, sink-only. No lookup/source side (JOIN-style reads from Qdrant) yet — may follow as v2 if
there's a real use case for it. No UPDATE/DELETE changelog support: Qdrant's upsert-by-point-ID
already gives idempotent "insert or replace" semantics, which covers the common case (write
embeddings once, re-write to update) without needing full changelog handling.

## Why a custom connector instead of just a `DataStream` job calling the Qdrant client directly

Same justification as ClickHouse: once you're doing this more than once, you want declarative
`CREATE TABLE ... WITH ('connector' = 'qdrant', ...)` + `INSERT INTO`, with batching, retries, and
type mapping handled once instead of re-implemented per job.

## Usage

```sql
CREATE TABLE embeddings (
    id       STRING,
    text     STRING,
    embedding ARRAY<FLOAT>
) WITH (
    'connector'       = 'qdrant',
    'host'            = 'localhost',
    'grpc-port'       = '6334',
    'collection-name' = 'my_collection',
    'vector-field'    = 'embedding',
    'id-field'        = 'id'
);

INSERT INTO embeddings SELECT id, text, embedding FROM source_stream;
```

### Options

| Option | Required | Default | Notes |
|---|---|---|---|
| `host` | yes | — | Qdrant server host |
| `grpc-port` | no | `6334` | Qdrant gRPC port |
| `use-tls` | no | `false` | |
| `api-key` | no | — | If the server requires auth |
| `collection-name` | yes | — | Must already exist — this connector doesn't create collections |
| `vector-field` | yes | — | Must be `ARRAY<FLOAT>` |
| `id-field` | no | — | `STRING` (UUID format) or `BIGINT`/`INTEGER`. If unset, a random UUID is generated per row (every row becomes a new point, never overwrites) |
| `sink.batch-size` | no | `64` | Rows buffered before an upsert call |
| `sink.flush-interval-ms` | no | `1000` | Max time a partial batch is held |
| `sink.max-retries` | no | `3` | Per-batch upsert retries before failing the job |

Every column other than `vector-field` and `id-field` becomes a Qdrant payload field. Supported
payload column types: `BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`,
`CHAR`, `VARCHAR`. Anything else (e.g. `DECIMAL`, `TIMESTAMP`, nested `ARRAY`/`ROW`) must be cast
or excluded before the sink — fails fast at job-submission time with a clear error, not silently
at runtime.

## Build

```
mvn clean install
```

- `flink-connector-qdrant/` — the connector itself, depends on Flink as `provided`.
- `flink-sql-connector-qdrant/` — shaded fat-jar bundling the Qdrant client + gRPC deps, for
  dropping into Flink's `lib/` or the SQL client's jar path.

## Test

```
mvn test          # unit tests only (row conversion, factory validation) — no Docker needed
mvn verify         # also runs the testcontainers IT against a real qdrant/qdrant container
```

**Known gap**: on this dev machine (native Windows JVM, not WSL2), `mvn verify`'s testcontainers IT
fails with `Could not find a valid Docker environment`, even though the Docker CLI and
`docker compose` both work fine — same class of issue as the mailpit/`make` gaps hit building
GoAlert's smoke tests: Windows' native npipe transport isn't reliably detected by
`docker-java`/testcontainers from a plain Windows JVM. Not yet root-caused. Should work as-is in
CI (Linux runners) or from WSL2.

**Verified end-to-end instead**, via the manual docker-compose path below: `CREATE TABLE ... WITH
('connector' = 'qdrant', ...)`, `INSERT INTO ... VALUES (...)` through the real Flink SQL client
against a real `flink:1.20` cluster and a real `qdrant/qdrant` container — both rows landed with
correct payload and vector.

**Real bug caught doing that verification, now fixed**: the first build of
`flink-sql-connector-qdrant`'s shaded jar silently broke gRPC name resolution. The shade plugin's
default behavior keeps only *one* dependency's copy of each `META-INF/services/io.grpc.*` file
instead of merging them (gRPC's `NameResolverProvider`/`ManagedChannelProvider`/`LoadBalancerProvider`
registries are all service-loader based, and several `io.grpc:*` jars each ship a fragment). That
silently dropped the `dns`/plain-host resolver and left a stray `unix` one as the only registered
provider, so every connection attempt to a plain `host:port` target failed with `Address types of
NameResolver 'unix' for 'qdrant:6334' not supported by transport` — a confusing error with no
obvious link back to the shading step. Fixed by adding `ServicesResourceTransformer` to the shade
plugin config (see `flink-sql-connector-qdrant/pom.xml`), which merges these files instead of
picking one. **Any future Flink connector here that shades gRPC-based client deps needs the same
transformer** — this isn't Qdrant-specific, it'll bite the DuckDB connector too if its client pulls
in gRPC (unlikely for JDBC, but worth remembering for any future gRPC-based connector).

## Manually trying it against a local Flink cluster

```
mvn clean package
docker compose up -d
# Flink UI: http://localhost:8081 ; Qdrant: http://localhost:6333/dashboard
docker compose exec jobmanager ./bin/sql-client.sh
```

Then, inside the SQL client, create a collection in Qdrant first (the connector doesn't do this
for you), point a `CREATE TABLE ... WITH ('connector' = 'qdrant', 'host' = 'qdrant', ...)` at it,
and `INSERT INTO ... VALUES (...)`.

## License

Apache License 2.0.
