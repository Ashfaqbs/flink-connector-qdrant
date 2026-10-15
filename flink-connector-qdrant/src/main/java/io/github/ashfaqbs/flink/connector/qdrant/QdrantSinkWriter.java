package io.github.ashfaqbs.flink.connector.qdrant;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Points.PointStruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Buffers incoming rows and upserts them to Qdrant in batches, flushing on batch-size, on a timer,
 * or when Flink asks for a final flush (checkpoint / end of input).
 *
 * <p>Upserts are by point ID, so retries after a transient failure are safe to redo in full — a
 * retried batch just overwrites the same points again.
 */
public final class QdrantSinkWriter implements SinkWriter<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(QdrantSinkWriter.class);

    private final QdrantWriterOptions options;
    private final QdrantRowConverter converter;
    private final QdrantClient client;
    private final List<PointStruct> buffer;

    private long lastFlushMillis;

    public QdrantSinkWriter(QdrantWriterOptions options, QdrantRowConverter converter) {
        this.options = options;
        this.converter = converter;
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
        flushBuffer();
        client.close();
    }

    private void flushBuffer() {
        if (buffer.isEmpty()) {
            lastFlushMillis = System.currentTimeMillis();
            return;
        }
        List<PointStruct> batch = new ArrayList<>(buffer);
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
            }
        }
    }
}
