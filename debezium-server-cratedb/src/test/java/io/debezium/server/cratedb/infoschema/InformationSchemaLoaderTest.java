/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.infoschema;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.server.cratedb.CrateTestResourceLifecycleManager;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test validating CrateDB behavior under certain conditions.
 *
 * @author Gabriel Habryn
 */
@QuarkusTest
@QuarkusTestResource(CrateTestResourceLifecycleManager.class)
class InformationSchemaLoaderTest {
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

            var infos = DataLoader.withTableName("test").load(conn);
            assertThat(infos.isEmpty()).isFalse();

            assertEquals(infos.size(), 5);
            for (ColumnInfo info : infos) {
                LOGGER.info("{}", info);
            }
            assertThat(infos.get(0)).isEqualTo(new ColumnInfo.Builder()
                    .setDataType("text")
                    .setColumnName("id")
                    .setColumnDetails(new ColumnDetails("id", List.of()))
                    .setIsPrimaryKey(true)
                    .build());

            assertThat(infos.get(1)).isEqualTo(new ColumnInfo.Builder()
                    .setDataType("object")
                    .setColumnName("doc")
                    .setColumnDetails(new ColumnDetails("doc", List.of()))
                    .build());

            assertThat(infos.get(2)).isEqualTo(new ColumnInfo.Builder()
                    .setDataType("text")
                    .setColumnName("doc['name']")
                    .setColumnDetails(new ColumnDetails("doc", List.of("name")))
                    .build());

            assertThat(infos.get(3)).isEqualTo(new ColumnInfo.Builder()
                    .setDataType("bigint_array")
                    .setColumnName("doc['name_bigint_array']")
                    .setColumnDetails(new ColumnDetails("doc", List.of("name_bigint_array")))
                    .build());

            assertThat(infos.get(4)).isEqualTo(new ColumnInfo.Builder()
                    .setDataType("boolean")
                    .setColumnName("doc['truth']")
                    .setColumnDetails(new ColumnDetails("doc", List.of("truth")))
                    .build());
        });
    }
}
