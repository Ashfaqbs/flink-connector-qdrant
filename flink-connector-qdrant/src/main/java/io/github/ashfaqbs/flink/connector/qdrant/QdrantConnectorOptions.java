package io.github.ashfaqbs.flink.connector.qdrant;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options accepted by {@code WITH (...)} on a table using {@code connector = 'qdrant'}. */
public final class QdrantConnectorOptions {

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Qdrant server host.");

    public static final ConfigOption<Integer> GRPC_PORT =
            ConfigOptions.key("grpc-port")
                    .intType()
                    .defaultValue(6334)
                    .withDescription("Qdrant gRPC port.");

    public static final ConfigOption<Boolean> USE_TLS =
            ConfigOptions.key("use-tls")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to use TLS for the gRPC connection.");

    public static final ConfigOption<String> API_KEY =
            ConfigOptions.key("api-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Qdrant API key, if the server requires authentication.");

    public static final ConfigOption<String> COLLECTION_NAME =
            ConfigOptions.key("collection-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target Qdrant collection name. Must already exist.");

    public static final ConfigOption<String> VECTOR_FIELD =
            ConfigOptions.key("vector-field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the ARRAY<FLOAT> column holding the embedding vector.");

    public static final ConfigOption<String> ID_FIELD =
            ConfigOptions.key("id-field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the STRING or BIGINT column to use as the Qdrant point ID. "
                                    + "If unset, a random UUID is generated per row (upserts will "
                                    + "never match an existing point).");

    public static final ConfigOption<Integer> SINK_BATCH_SIZE =
            ConfigOptions.key("sink.batch-size")
                    .intType()
                    .defaultValue(64)
                    .withDescription("Number of points to buffer before an upsert call.");

    public static final ConfigOption<Long> SINK_FLUSH_INTERVAL_MS =
            ConfigOptions.key("sink.flush-interval-ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            "Max time a batch is held before it's flushed, even if not full.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Max retries for a failed upsert batch before failing.");

    private QdrantConnectorOptions() {}
}
