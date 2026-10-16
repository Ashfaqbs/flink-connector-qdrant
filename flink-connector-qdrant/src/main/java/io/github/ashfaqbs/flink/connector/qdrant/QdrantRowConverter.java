package io.github.ashfaqbs.flink.connector.qdrant;

import static io.qdrant.client.PointIdFactory.id;
import static io.qdrant.client.ValueFactory.value;
import static io.qdrant.client.VectorsFactory.vectors;

import io.qdrant.client.grpc.Points.PointStruct;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/**
 * Converts a Flink {@link RowData} into a Qdrant {@link PointStruct}: one column becomes the
 * vector, one optional column becomes the point ID, everything else becomes payload.
 */
public final class QdrantRowConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int vectorFieldIndex;
    private final int idFieldIndex; // -1 if not configured
    private final LogicalTypeRoot idFieldTypeRoot;
    private final List<PayloadField> payloadFields;

    public QdrantRowConverter(RowType rowType, String vectorField, String idField) {
        List<String> names = rowType.getFieldNames();
        this.vectorFieldIndex = names.indexOf(vectorField);
        if (vectorFieldIndex < 0) {
            throw new IllegalArgumentException(
                    "vector-field '" + vectorField + "' not found in schema: " + names);
        }
        if (idField != null) {
            this.idFieldIndex = names.indexOf(idField);
            if (idFieldIndex < 0) {
                throw new IllegalArgumentException(
                        "id-field '" + idField + "' not found in schema: " + names);
            }
            this.idFieldTypeRoot = rowType.getTypeAt(idFieldIndex).getTypeRoot();
            if (!isSupportedIdType(idFieldTypeRoot)) {
                throw new IllegalArgumentException(
                        "id-field '"
                                + idField
                                + "' has unsupported type "
                                + rowType.getTypeAt(idFieldIndex)
                                + ". Supported id-field types: STRING/VARCHAR (must contain a "
                                + "UUID, e.g. '5c56c793-69f3-4fbf-87e6-c4bf54c28c26'), BIGINT, "
                                + "INTEGER.");
            }
        } else {
            this.idFieldIndex = -1;
            this.idFieldTypeRoot = null;
        }

        this.payloadFields = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            if (i == vectorFieldIndex || i == idFieldIndex) {
                continue;
            }
            LogicalType fieldType = rowType.getTypeAt(i);
            if (!isSupportedPayloadType(fieldType.getTypeRoot())) {
                throw new IllegalArgumentException(
                        "Unsupported payload column type "
                                + fieldType
                                + " for field '"
                                + names.get(i)
                                + "'. Supported payload types: BOOLEAN, TINYINT, SMALLINT, "
                                + "INTEGER, BIGINT, FLOAT, DOUBLE, CHAR, VARCHAR. Cast or "
                                + "exclude this column before the sink.");
            }
            payloadFields.add(new PayloadField(i, names.get(i), fieldType));
        }
    }

    private static boolean isSupportedIdType(LogicalTypeRoot root) {
        return root == LogicalTypeRoot.VARCHAR
                || root == LogicalTypeRoot.CHAR
                || root == LogicalTypeRoot.BIGINT
                || root == LogicalTypeRoot.INTEGER;
    }

    private static boolean isSupportedPayloadType(LogicalTypeRoot root) {
        switch (root) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
                return true;
            default:
                return false;
        }
    }

    public PointStruct toPoint(RowData row) {
        if (row.isNullAt(vectorFieldIndex)) {
            throw new IllegalStateException(
                    "vector-field is null for a row -- the vector column is mandatory and has "
                            + "no sensible default. Filter out null-vector rows upstream of "
                            + "this sink.");
        }
        PointStruct.Builder builder =
                PointStruct.newBuilder()
                        .setId(resolveId(row))
                        .setVectors(vectors(toFloatArray(row.getArray(vectorFieldIndex))));

        for (PayloadField field : payloadFields) {
            Object javaValue = extractJavaValue(row, field.index, field.type);
            if (javaValue != null) {
                builder.putPayload(field.name, toQdrantValue(javaValue));
            }
        }
        return builder.build();
    }

    private io.qdrant.client.grpc.Points.PointId resolveId(RowData row) {
        if (idFieldIndex < 0) {
            return id(UUID.randomUUID());
        }
        if (row.isNullAt(idFieldIndex)) {
            return id(UUID.randomUUID());
        }
        if (idFieldTypeRoot == LogicalTypeRoot.BIGINT) {
            return id(row.getLong(idFieldIndex));
        }
        if (idFieldTypeRoot == LogicalTypeRoot.INTEGER) {
            return id((long) row.getInt(idFieldIndex));
        }
        // VARCHAR/CHAR: Qdrant point IDs must be an unsigned integer or a UUID string.
        String raw = row.getString(idFieldIndex).toString();
        try {
            return id(UUID.fromString(raw));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "id-field value '"
                            + raw
                            + "' is not a valid UUID. Qdrant point IDs from a STRING column "
                            + "must be formatted as a UUID, e.g. "
                            + "'5c56c793-69f3-4fbf-87e6-c4bf54c28c26'.",
                    e);
        }
    }

    private static float[] toFloatArray(ArrayData array) {
        int size = array.size();
        float[] result = new float[size];
        for (int i = 0; i < size; i++) {
            result[i] = array.getFloat(i);
        }
        return result;
    }

    private static Object extractJavaValue(RowData row, int index, LogicalType type) {
        if (row.isNullAt(index)) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return row.getBoolean(index);
            case TINYINT:
                return (long) row.getByte(index);
            case SMALLINT:
                return (long) row.getShort(index);
            case INTEGER:
                return (long) row.getInt(index);
            case BIGINT:
                return row.getLong(index);
            case FLOAT:
                return (double) row.getFloat(index);
            case DOUBLE:
                return row.getDouble(index);
            case CHAR:
            case VARCHAR:
                return row.getString(index).toString();
            default:
                // Unreachable: the constructor rejects unsupported types up front.
                throw new IllegalStateException("Unsupported payload type: " + type);
        }
    }

    private static io.qdrant.client.grpc.JsonWithInt.Value toQdrantValue(Object javaValue) {
        if (javaValue instanceof Boolean b) {
            return value(b);
        }
        if (javaValue instanceof Long l) {
            return value(l);
        }
        if (javaValue instanceof Double d) {
            return value(d);
        }
        return value(String.valueOf(javaValue));
    }

    private static final class PayloadField implements Serializable {
        private static final long serialVersionUID = 1L;
        final int index;
        final String name;
        final LogicalType type;

        PayloadField(int index, String name, LogicalType type) {
            this.index = index;
            this.name = name;
            this.type = type;
        }
    }
}
