/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.infoschema;

import io.debezium.server.cratedb.CrateTestResourceLifecycleManager;
import io.debezium.server.cratedb.schema.Schema;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@QuarkusTest
@QuarkusTestResource(CrateTestResourceLifecycleManager.class)
class SchemaBuilderTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(InformationSchemaLoaderTest.class);

    private static java.sql.Connection conn = null;

    @BeforeEach
    void setup() throws Exception {
        // Initialize the connection
        conn = java.sql.DriverManager.getConnection(CrateTestResourceLifecycleManager.getUrl());
    }

    @AfterEach
    void tearDown() throws Exception {
        // Close the connection if it's not null and not closed
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    @Test
    void testFromInformationSchemaWithEmptyColumnsList() {
        List<ColumnInfo> columns = new ArrayList<>();
        Schema.I result =  SchemaBuilder.fromInformationSchema(columns);
        assertThat(result).isEqualTo(Schema.Dict.of());
    }

    @Test
    void testFromInformationSchemaWithValidColumnsList() {
        ColumnInfo.Builder builder = new ColumnInfo.Builder();
        ColumnInfo columnInfo = builder.setColumnName("id").setDataType("integer").build();
        List<ColumnInfo> columns = List.of(columnInfo);
        Schema.I result = SchemaBuilder.fromInformationSchema(columns);
        assertThat(result).isEqualTo(Schema.Dict.of(
            "id", Schema.Primitive.BIGINT
        ));
    }

    @Test
    void withTableName() {
        assertDoesNotThrow(() -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS test");
                stmt.execute("CREATE TABLE test (id TEXT PRIMARY KEY, doc OBJECT)");
                stmt.execute("INSERT INTO test VALUES ('1', {name = 'gab'})");
                stmt.execute("INSERT INTO test VALUES ('2', {name_bigint_array = [1,2,3]})");
                stmt.execute("INSERT INTO test VALUES ('3', {truth = false})");
                stmt.execute("REFRESH TABLE test");
            }

            var columns = DataLoader.withTableName("test").load(conn);
            assertThat(columns.isEmpty()).isFalse();

            assertEquals(columns.size(), 5);
            for (ColumnInfo column : columns) {
                LOGGER.info("{}", column);
            }

            Schema.I result = SchemaBuilder.fromInformationSchema(columns);
            LOGGER.info("result={}", result);
            assertThat(result).isEqualTo(Schema.Dict.of(
                    "id", Schema.Primitive.TEXT,
                    "doc", Schema.Dict.of(
                            "name", Schema.Primitive.TEXT,
                            "name_bigint_array", Schema.Array.of(Schema.Primitive.BIGINT),
                            "truth", Schema.Primitive.BOOLEAN
                    )
            ));
        });
    }
}
