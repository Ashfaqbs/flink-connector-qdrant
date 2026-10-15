package io.github.ashfaqbs.flink.connector.qdrant;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

/** A sink that upserts rows into a Qdrant collection. No committer: upserts are idempotent by
 * point ID, so there's nothing to make exactly-once beyond retrying the whole batch. */
public final class QdrantSink implements Sink<RowData> {

    private static final long serialVersionUID = 1L;

    private final QdrantWriterOptions options;
    private final QdrantRowConverter converter;

    public QdrantSink(QdrantWriterOptions options, QdrantRowConverter converter) {
        this.options = options;
        this.converter = converter;
    }

    @Override
    public SinkWriter<RowData> createWriter(InitContext context) {
        return new QdrantSinkWriter(options, converter);
    }
}
