package io.github.ashfaqbs.flink.connector.qdrant;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.logical.RowType;

/** {@link DynamicTableSink} for Qdrant: insert-only (upserts are keyed by point ID, not by
 * Flink's changelog semantics, so update/delete rows are not supported in v1). */
public final class QdrantDynamicTableSink implements DynamicTableSink {

    private final QdrantWriterOptions writerOptions;
    private final RowType rowType;
    private final String vectorField;
    private final String idField;

    public QdrantDynamicTableSink(
            QdrantWriterOptions writerOptions, RowType rowType, String vectorField, String idField) {
        this.writerOptions = writerOptions;
        this.rowType = rowType;
        this.vectorField = vectorField;
        this.idField = idField;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        QdrantRowConverter converter = new QdrantRowConverter(rowType, vectorField, idField);
        return SinkV2Provider.of(new QdrantSink(writerOptions, converter));
    }

    @Override
    public DynamicTableSink copy() {
        return new QdrantDynamicTableSink(writerOptions, rowType, vectorField, idField);
    }

    @Override
    public String asSummaryString() {
        return "Qdrant";
    }
}
