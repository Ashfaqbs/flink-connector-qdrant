package io.github.ashfaqbs.flink.connector.qdrant;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.JsonWithInt.Value;
import io.qdrant.client.grpc.Points.PointId;
import io.qdrant.client.grpc.Points.PointStruct;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Buffers incoming rows and upserts them to Qdrant in batches, flushing on batch-size, when the
 * flush interval has elapsed by the time the next row arrives (checked opportunistically in
 * {@link #write}, not on a background timer -- a quiet stream with checkpointing disabled can
 * leave a partial batch unflushed), or when Flink asks for a final flush (checkpoint / end of
 * input).
 *
 * <p>Upserts are by point ID, so retries after a transient failure are safe to redo in full — a
 * retried batch just overwrites the same points again.
 */
public final class QdrantSinkWriter implements SinkWriter<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(QdrantSinkWriter.class);

    private final QdrantWriterOptions options;
    private final QdrantRowConverter converter;
    private final String versionField;
    private final QdrantClient client;
    private final List<PointStruct> buffer;

    private long lastFlushMillis;

    public QdrantSinkWriter(QdrantWriterOptions options, QdrantRowConverter converter) {
        this(options, converter, null);
    }

    public QdrantSinkWriter(
            QdrantWriterOptions options, QdrantRowConverter converter, String versionField) {
        this.options = options;
        this.converter = converter;
        this.versionField = versionField;
        this.buffer = new ArrayList<>(options.batchSize());
        this.lastFlushMillis = System.currentTimeMillis();

        QdrantGrpcClient.Builder grpcBuilder =
                QdrantGrpcClient.newBuilder(options.host(), options.grpcPort(), options.useTls());
        if (options.apiKey() != null && !options.apiKey().isEmpty()) {
            grpcBuilder.withApiKey(options.apiKey());
        }
        this.client = new QdrantClient(grpcBuilder.build());
    }

    @Override
    public void write(RowData element, Context context) {
        buffer.add(converter.toPoint(element));
        if (buffer.size() >= options.batchSize()
                || System.currentTimeMillis() - lastFlushMillis >= options.flushIntervalMs()) {
            flushBuffer();
        }
    }

    @Override
    public void flush(boolean endOfInput) {
        flushBuffer();
    }

    @Override
    public void close() {
        try {
            flushBuffer();
        } finally {
            client.close();
        }
    }

    private void flushBuffer() {
        if (buffer.isEmpty()) {
            lastFlushMillis = System.currentTimeMillis();
            return;
        }
        List<PointStruct> batch = dedupeByVersion(buffer);
        buffer.clear();
        lastFlushMillis = System.currentTimeMillis();

        int attempt = 0;
        while (true) {
            try {
                client.upsertAsync(options.collectionName(), batch).get();
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while upserting to Qdrant", e);
            } catch (ExecutionException e) {
                attempt++;
                if (attempt > options.maxRetries()) {
                    throw new RuntimeException(
                            "Failed to upsert "
                                    + batch.size()
                                    + " point(s) to Qdrant collection '"
                                    + options.collectionName()
                                    + "' after "
                                    + attempt
                                    + " attempt(s)",
                            e);
                }
                LOG.warn(
                        "Upsert attempt {} of {} failed for {} point(s), retrying",
                        attempt,
                        options.maxRetries(),
                        batch.size(),
                        e);
                backoffBeforeRetry(attempt);
            }
        }
    }

    /**
     * If {@code versionField} is configured and the same point ID appears more than once in
     * {@code pending} (e.g. a retried batch re-buffered alongside newer rows for the same key),
     * keeps only the row with the highest value for that payload field, so arrival order within
     * the batch can't let a stale row overwrite a newer one. A row missing the field, or with a
     * non-numeric value for it, is treated as the oldest possible version. This only orders
     * duplicates inside one buffered batch -- it cannot order writes across separate batches or
     * concurrent writers, since Qdrant has no server-side conditional/CAS write.
     */
    List<PointStruct> dedupeByVersion(List<PointStruct> pending) {
        if (versionField == null) {
            return new ArrayList<>(pending);
        }
        Map<PointId, PointStruct> winners = new LinkedHashMap<>();
        Map<PointId, Double> winnerVersions = new LinkedHashMap<>();
        for (PointStruct point : pending) {
            PointId id = point.getId();
            double version = extractVersion(point);
            Double existing = winnerVersions.get(id);
            if (existing == null || version >= existing) {
                winners.put(id, point);
                winnerVersions.put(id, version);
            }
        }
        return new ArrayList<>(winners.values());
    }

    private double extractVersion(PointStruct point) {
        Value value = point.getPayloadMap().get(versionField);
        if (value == null) {
            return Double.NEGATIVE_INFINITY;
        }
        if (value.hasIntegerValue()) {
            return value.getIntegerValue();
        }
        if (value.hasDoubleValue()) {
            return value.getDoubleValue();
        }
        return Double.NEGATIVE_INFINITY;
    }

    private static void backoffBeforeRetry(int attempt) {
        // Exponential backoff (200ms, 400ms, 800ms, ...) capped at 5s, so a struggling Qdrant
        // instance isn't hammered with immediate retries during the exact window it's failing.
        long delayMs = Math.min(200L * (1L << Math.min(attempt, 10)), 5000L);
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while backing off before an upsert retry", e);
        }
    }
}
