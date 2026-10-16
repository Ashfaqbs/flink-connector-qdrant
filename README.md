# flink-connector-qdrant

[![CI](https://github.com/Ashfaqbs/flink-connector-qdrant/actions/workflows/ci.yml/badge.svg)](https://github.com/Ashfaqbs/flink-connector-qdrant/actions/workflows/ci.yml)

**Write Apache Flink SQL results straight into [Qdrant](https://qdrant.tech) — no custom
`DataStream` job, no hand-rolled client plumbing.**

```sql
CREATE TABLE embeddings (id STRING, text STRING, embedding ARRAY<FLOAT>)
WITH ('connector' = 'qdrant', 'host' = 'localhost', 'collection-name' = 'my_collection',
      'vector-field' = 'embedding', 'id-field' = 'id');

INSERT INTO embeddings SELECT id, text, embedding FROM source_stream;
```

That's it — Flink does the streaming, this connector handles batching, retries, and the
RowData-to-Qdrant-point mapping.

License: Apache 2.0 · Targets Flink **1.20 (LTS)** · Status: **v1, sink-only**

---

## Why this exists

Flink increasingly sits in front of RAG and embedding pipelines — compute an embedding, write it
somewhere a vector search can find it. Qdrant is one of the most widely used vector databases, and
as of this writing there is no official Flink connector for it, and no well-known community one
either — a widely-used store, a widely-used streaming engine, and nothing connecting them in SQL.
This connector closes that gap with a proper dynamic table factory, not a one-off `DataStream` job
glued together per project.

## Quickstart

```bash
git clone https://github.com/Ashfaqbs/flink-connector-qdrant.git && cd flink-connector-qdrant
mvn clean install
```

Try it against a local Flink cluster + Qdrant with one command:

```bash
docker compose up -d
# Flink UI: http://localhost:8081 · Qdrant dashboard: http://localhost:6333/dashboard
```

Create a collection, then run the SQL from the top of this README via
`docker compose exec jobmanager ./bin/sql-client.sh`. Full walkthrough in
[Manually trying it](#manually-trying-it-against-a-local-flink-cluster) below.

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

One column maps to the vector, one optional column maps to the Qdrant point ID, every other
column becomes a Qdrant payload field automatically — no manual mapping needed.

### Options

| Option | Required | Default | Notes |
|---|---|---|---|
| `host` | yes | — | Qdrant server host |
| `grpc-port` | no | `6334` | Qdrant gRPC port |
| `use-tls` | no | `false` | |
| `api-key` | no | — | If the server requires auth |
| `collection-name` | yes | — | Must already exist — this connector doesn't create collections |
| `vector-field` | yes | — | Must be `ARRAY<FLOAT>` |
| `id-field` | no | — | `STRING` (must contain a UUID), `BIGINT`, or `INTEGER`. If unset, a random UUID is generated per row (every row becomes a new point, never overwrites) |
| `sink.batch-size` | no | `64` | Rows buffered before an upsert call |
| `sink.flush-interval-ms` | no | `1000` | Max time a partial batch is held |
| `sink.max-retries` | no | `3` | Per-batch upsert retries (exponential backoff) before failing the job |

Supported payload column types: `BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`,
`DOUBLE`, `CHAR`, `VARCHAR`. Anything else (e.g. `DECIMAL`, `TIMESTAMP`, nested `ARRAY`/`ROW`) must
be cast or excluded before the sink — this fails fast with a clear error at SQL-planning time, not
silently at runtime.

## Status / limitations

- **Sink only** — no lookup/source side (JOIN-style reads from Qdrant) yet. Open an issue if you
  need one; it may follow as v2.
- **No UPDATE/DELETE changelog support** — Qdrant's upsert-by-point-ID already gives idempotent
  "insert or replace" semantics, which covers the common case (write once, re-write to update)
  without full changelog handling.
- **Collection must already exist** — this connector doesn't create it for you.

## Build & test

```bash
mvn clean install   # builds both modules
mvn test             # unit tests only (no Docker needed) — 13/13 passing
mvn verify            # also runs the testcontainers IT against a real qdrant/qdrant container
```

- `flink-connector-qdrant/` — the connector itself, depends on Flink as `provided`.
- `flink-sql-connector-qdrant/` — shaded fat-jar bundling the Qdrant client + gRPC deps, for
  dropping into Flink's `lib/` or the SQL client's jar path.

> **Note**: if you change code in `flink-connector-qdrant` and rebuild, use `mvn install` (not
> just `package`) before testing the shaded jar in `flink-sql-connector-qdrant/target/` — a plain
> `package` across both modules can silently repackage a stale cached copy of the connector jar
> from `~/.m2` instead of your fresh build.

## Manually trying it against a local Flink cluster

```bash
mvn clean package
docker compose up -d
curl -X PUT http://localhost:6333/collections/my_collection \
    -H 'Content-Type: application/json' \
    -d '{"vectors": {"size": 3, "distance": "Cosine"}}'
docker compose exec jobmanager ./bin/sql-client.sh
```

Then, inside the SQL client, run the `CREATE TABLE` + `INSERT INTO` from the top of this README
(use `'host' = 'qdrant'` instead of `localhost` — that's the hostname on the compose network).

## Development notes

<details>
<summary>A real gRPC shading bug caught while verifying this end-to-end (click to expand)</summary>

The first build of `flink-sql-connector-qdrant`'s shaded jar silently broke gRPC name resolution.
The shade plugin's default behavior keeps only *one* dependency's copy of each
`META-INF/services/io.grpc.*` file instead of merging them (gRPC's
`NameResolverProvider`/`ManagedChannelProvider`/`LoadBalancerProvider` registries are all
service-loader based, and several `io.grpc:*` jars each ship a fragment). That silently dropped
the `dns`/plain-host resolver and left a stray `unix` one as the only registered provider, so
every connection attempt to a plain `host:port` target failed with `Address types of NameResolver
'unix' for 'qdrant:6334' not supported by transport` — a confusing error with no obvious link back
to the shading step. Fixed by adding `ServicesResourceTransformer` to the shade plugin config (see
`flink-sql-connector-qdrant/pom.xml`), which merges these files instead of picking one. Any future
Flink connector that shades a gRPC-based client needs the same transformer.

</details>

<details>
<summary>Known test gap on native Windows (click to expand)</summary>

On a native Windows JVM (not WSL2), `mvn verify`'s testcontainers IT fails with `Could not find a
valid Docker environment`, even though the Docker CLI and `docker compose` both work fine —
Windows' native npipe transport isn't reliably detected by `docker-java`/testcontainers from a
plain Windows JVM. Not yet root-caused. Works fine in CI (Linux runners) or from WSL2. The
connector has still been verified end-to-end manually (see above) against a real Flink cluster and
a real Qdrant container.

</details>

## License

Apache License 2.0.
