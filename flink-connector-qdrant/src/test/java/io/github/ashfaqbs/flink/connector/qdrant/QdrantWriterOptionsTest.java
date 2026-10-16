package io.github.ashfaqbs.flink.connector.qdrant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class QdrantWriterOptionsTest {

    @Test
    void rejectsNonPositiveBatchSize() {
        assertThatThrownBy(() -> new QdrantWriterOptions("h", 6334, false, null, "c", 0, 1000L, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch-size");
    }

    @Test
    void rejectsNegativeFlushInterval() {
        assertThatThrownBy(() -> new QdrantWriterOptions("h", 6334, false, null, "c", 10, -1L, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flush-interval-ms");
    }

    @Test
    void rejectsNegativeMaxRetries() {
        assertThatThrownBy(() -> new QdrantWriterOptions("h", 6334, false, null, "c", 10, 1000L, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-retries");
    }
}
