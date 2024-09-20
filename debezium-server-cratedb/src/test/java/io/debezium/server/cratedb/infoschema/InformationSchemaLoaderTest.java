/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.infoschema;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import io.debezium.server.cratedb.ColumnName;
import io.debezium.server.cratedb.ColumnTypeManager;
import io.debezium.server.cratedb.CrateTestResourceLifecycleManager;
import io.debezium.server.cratedb.datagen.DataGen;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

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

    @Test
    void withGenerated() {
        assertDoesNotThrow(() -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS test");
                stmt.execute("CREATE TABLE test (id TEXT PRIMARY KEY, doc OBJECT)");
                stmt.execute("REFRESH TABLE test");
            }

            var infos1 = DataLoader.withTableName("test").load(conn);
            assertThat(infos1.isEmpty()).isFalse();
            for (ColumnInfo info : infos1) {
                LOGGER.info("{}", info);
            }

            ColumnTypeManager manager1 = new ColumnTypeManager();
            manager1.fromInformationSchema(infos1);
            manager1.print();

            ObjectMapper mapper = new ObjectMapper();
            List<Object> generated = new ArrayList<>();

            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test(id, doc) VALUES (?, ?)")) {
                for (int i = 0; i < 10; i++) {
                    Object object = DataGen.generateObject();
                    generated.add(object);

                    LOGGER.error("BEFORE: {}", object);

                    Object normalizedObject = manager1.fromObject(object, manager1.getObjectType(new ColumnName("doc")));

                    var nested = manager1.extractNestedArrayTypes(manager1.getSchema());
                    var alters = manager1.printAlterTable("test", nested);
                    for (String alter : alters) {
                        LOGGER.error("ALTER: {}", alter);
                        try (Statement stmt2 = conn.createStatement()) {
                            try {
                                stmt2.execute(alter);
                            }
                            catch (Exception e) {
                                LOGGER.error("ALTER EXCEPTION {}", e.getMessage());
                            }
                            stmt2.execute("REFRESH TABLE test");
                        }
                    }

                    String json = mapper.writeValueAsString(normalizedObject);

                    LOGGER.error("POST: {}", json);
                    stmt.setString(1, String.valueOf(i));
                    stmt.setString(2, json);
                    try {
                        stmt.execute();
                    }
                    catch (Exception e) {
                        LOGGER.error("BEFORE EXCEPTION:");
                        manager1.print();
                        throw e;
                    }

                    // x ERROR: Dynamic nested arrays are not supported
                    // √ ERROR: "name_." contains a dot
                    // √ ERROR: "[" conflicts with subscript pattern, square brackets are not allowed
                    // √ ERROR: Mixed dataTypes inside a list are not supported. Found object_array and boolean
                }
            }

            LOGGER.info("Generated: {}", generated);
            manager1.print();

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("REFRESH TABLE test");
            }

            var infos2 = DataLoader.withTableName("test").load(conn);
            assertThat(infos2.isEmpty()).isFalse();

            for (ColumnInfo info : infos2) {
                LOGGER.info("{}", info);
            }

            ColumnTypeManager manager2 = new ColumnTypeManager();
            manager2.fromInformationSchema(infos2);
            manager2.print();
        });
    }
}
