package io.github.ashfaqbs.flink.connector.qdrant;

import static org.assertj.core.api.Assertions.assertThat;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections.Distance;
import io.qdrant.client.grpc.Collections.VectorParams;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Integration test against a real Qdrant instance via testcontainers. Run with {@code mvn
 * verify} — excluded from the default {@code test} goal (see surefire config in this module's
 * pom.xml). Requires a Docker daemon available on the host running the build. */
@Testcontainers
class QdrantSinkWriterIT {

    private static final String COLLECTION = "flink_it_test";

    @Container
    private final GenericContainer<?> qdrant =
            new GenericContainer<>(DockerImageName.parse("qdrant/qdrant:latest"))
                    .withExposedPorts(6333, 6334);

    private QdrantClient adminClient;

    @BeforeEach
    void setUp() {
        qdrant.start();
        adminClient =
                new QdrantClient(
                        QdrantGrpcClient.newBuilder(
                                        qdrant.getHost(), qdrant.getMappedPort(6334), false)
                                .build());
        try {
            adminClient
                    .createCollectionAsync(
                            COLLECTION,
                            VectorParams.newBuilder()
                                    .setSize(3)
                                    .setDistance(Distance.Cosine)
                                    .build())
                    .get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test collection", e);
        }
    }

    @AfterEach
    void tearDown() {
        adminClient.close();
        qdrant.stop();
    }

    @Test
    void writesRowsAndTheyAreRetrievableByPointId() throws Exception {
        RowType schema =
                RowType.of(
                        new LogicalType[] {
                            new VarCharType(VarCharType.MAX_LENGTH), new ArrayType(new FloatType())
                        },
                        new String[] {"id", "embedding"});
        QdrantRowConverter converter = new QdrantRowConverter(schema, "embedding", "id");
        QdrantWriterOptions options =
                new QdrantWriterOptions(
                        qdrant.getHost(),
                        qdrant.getMappedPort(6334),
                        false,
                        null,
                        COLLECTION,
                        10,
                        200L,
                        2);

        String pointId = "5c56c793-69f3-4fbf-87e6-c4bf54c28c26";
        try (QdrantSinkWriter writer = new QdrantSinkWriter(options, converter)) {
            writer.write(
                    GenericRowData.of(
                            StringData.fromString(pointId),
                            new GenericArrayData(new float[] {0.1f, 0.2f, 0.3f})),
                    null);
            writer.flush(true);
        }

        long count = adminClient.countAsync(COLLECTION).get();
        assertThat(count).isEqualTo(1L);
    }
}
