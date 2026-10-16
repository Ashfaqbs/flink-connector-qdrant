package io.github.ashfaqbs.flink.connector.qdrant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/** Exercises the retry-exhaustion path without needing a real Qdrant instance -- an unreachable
 * local port fails fast (connection refused) and deterministically, unlike a timeout-based
 * failure, so this doesn't need Docker/testcontainers. */
class QdrantSinkWriterTest {

    private static final RowType SCHEMA =
            RowType.of(
                    new LogicalType[] {
                        new VarCharType(VarCharType.MAX_LENGTH), new ArrayType(new FloatType())
                    },
                    new String[] {"id", "embedding"});

    @Test
    void exhaustsRetriesAndThrowsClearlyWhenUnreachable() {
        QdrantRowConverter converter = new QdrantRowConverter(SCHEMA, "embedding", "id");
        // Nothing listens on this port -- simulates Qdrant being down for the whole test.
        // batch-size and flush-interval set high enough that write() itself never incidentally
        // triggers the flush -- the explicit flush(true) call below is what we're asserting on.
        QdrantWriterOptions options =
                new QdrantWriterOptions("localhost", 61999, false, null, "whatever", 10, 60_000L, 2);

        try (QdrantSinkWriter writer = new QdrantSinkWriter(options, converter)) {
            writer.write(
                    GenericRowData.of(
                            StringData.fromString("5c56c793-69f3-4fbf-87e6-c4bf54c28c26"),
                            new GenericArrayData(new float[] {0.1f, 0.2f, 0.3f})),
                    null);
            assertThatThrownBy(() -> writer.flush(true))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("whatever")
                    .hasMessageContaining("3 attempt");
        }
    }
}
