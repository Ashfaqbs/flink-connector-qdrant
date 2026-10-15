package io.github.ashfaqbs.flink.connector.qdrant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.qdrant.client.grpc.Points.PointStruct;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class QdrantRowConverterTest {

    private static RowType schema() {
        return RowType.of(
                new org.apache.flink.table.types.logical.LogicalType[] {
                    new VarCharType(VarCharType.MAX_LENGTH), // id
                    new ArrayType(new FloatType()), // embedding
                    new VarCharType(VarCharType.MAX_LENGTH), // text
                    new BigIntType() // ts
                },
                new String[] {"id", "embedding", "text", "ts"});
    }

    @Test
    void convertsVectorIdAndPayload() {
        QdrantRowConverter converter = new QdrantRowConverter(schema(), "embedding", "id");

        GenericRowData row =
                GenericRowData.of(
                        StringData.fromString("5c56c793-69f3-4fbf-87e6-c4bf54c28c26"),
                        new GenericArrayData(new float[] {0.1f, 0.2f, 0.3f}),
                        StringData.fromString("hello"),
                        42L);

        PointStruct point = converter.toPoint(row);

        assertThat(point.getVectors().getVector().getDataList()).containsExactly(0.1f, 0.2f, 0.3f);
        assertThat(point.getId().getUuid()).isEqualTo("5c56c793-69f3-4fbf-87e6-c4bf54c28c26");
        assertThat(point.getPayloadMap().get("text").getStringValue()).isEqualTo("hello");
        assertThat(point.getPayloadMap().get("ts").getIntegerValue()).isEqualTo(42L);
    }

    @Test
    void generatesRandomIdWhenIdFieldNotConfigured() {
        QdrantRowConverter converter = new QdrantRowConverter(schema(), "embedding", null);

        GenericRowData row =
                GenericRowData.of(
                        StringData.fromString("ignored"),
                        new GenericArrayData(new float[] {1f}),
                        StringData.fromString("x"),
                        1L);

        PointStruct point = converter.toPoint(row);

        assertThat(point.getId().getUuid()).isNotBlank();
    }

    @Test
    void rejectsUnknownVectorField() {
        assertThatThrownBy(() -> new QdrantRowConverter(schema(), "does-not-exist", "id"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does-not-exist");
    }

    @Test
    void rejectsUnsupportedPayloadType() {
        RowType withArrayPayload =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            new ArrayType(new FloatType()), new ArrayType(new BigIntType())
                        },
                        new String[] {"embedding", "nested"});

        assertThatThrownBy(() -> new QdrantRowConverter(withArrayPayload, "embedding", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nested");
    }
}
