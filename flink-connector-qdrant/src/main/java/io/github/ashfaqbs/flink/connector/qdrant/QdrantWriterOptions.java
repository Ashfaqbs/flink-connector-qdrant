package io.github.ashfaqbs.flink.connector.qdrant;

import java.io.Serializable;
import java.util.Objects;

/** Immutable, serializable bundle of everything the sink writer needs at runtime. */
public final class QdrantWriterOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final int grpcPort;
    private final boolean useTls;
    private final String apiKey;
    private final String collectionName;
    private final int batchSize;
    private final long flushIntervalMs;
    private final int maxRetries;

    public QdrantWriterOptions(
            String host,
            int grpcPort,
            boolean useTls,
            String apiKey,
            String collectionName,
            int batchSize,
            long flushIntervalMs,
            int maxRetries) {
        this.host = Objects.requireNonNull(host, "host");
        this.grpcPort = grpcPort;
        this.useTls = useTls;
        this.apiKey = apiKey;
        this.collectionName = Objects.requireNonNull(collectionName, "collectionName");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("sink.batch-size must be positive, got " + batchSize);
        }
        if (flushIntervalMs < 0) {
            throw new IllegalArgumentException(
                    "sink.flush-interval-ms must not be negative, got " + flushIntervalMs);
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException(
                    "sink.max-retries must not be negative, got " + maxRetries);
        }
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.maxRetries = maxRetries;
    }

    public String host() {
        return host;
    }

    public int grpcPort() {
        return grpcPort;
    }

    public boolean useTls() {
        return useTls;
    }

    public String apiKey() {
        return apiKey;
    }

    public String collectionName() {
        return collectionName;
    }

    public int batchSize() {
        return batchSize;
    }

    public long flushIntervalMs() {
        return flushIntervalMs;
    }

    public int maxRetries() {
        return maxRetries;
    }
}
