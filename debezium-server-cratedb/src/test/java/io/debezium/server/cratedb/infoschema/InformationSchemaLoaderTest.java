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
import java.util.Map;

import io.debezium.server.cratedb.ColumnName;
import io.debezium.server.cratedb.ColumnTypeManager;
import io.debezium.server.cratedb.CrateTestResourceLifecycleManager;
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

            var infos = InformationSchemaLoader.withTableName("test").load(conn);
            assertThat(infos.isEmpty()).isFalse();

            assertEquals(infos.size(), 5);
            for (InformationSchemaColumnInfo info : infos) {
                LOGGER.info("{}", info);
            }
            assertThat(infos.get(0)).isEqualTo(new InformationSchemaColumnInfo.Builder()
                    .setDataType("text")
                    .setColumnName("id")
                    .setColumnDetails(new InformationSchemaColumnDetails("id", List.of()))
                    .setIsPrimaryKey(true)
                    .build());

            assertThat(infos.get(1)).isEqualTo(new InformationSchemaColumnInfo.Builder()
                    .setDataType("object")
                    .setColumnName("doc")
                    .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of()))
                    .build());

            assertThat(infos.get(2)).isEqualTo(new InformationSchemaColumnInfo.Builder()
                    .setDataType("text")
                    .setColumnName("doc['name']")
                    .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("name")))
                    .build());

            assertThat(infos.get(3)).isEqualTo(new InformationSchemaColumnInfo.Builder()
                    .setDataType("bigint_array")
                    .setColumnName("doc['name_bigint_array']")
                    .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("name_bigint_array")))
                    .build());

            assertThat(infos.get(4)).isEqualTo(new InformationSchemaColumnInfo.Builder()
                    .setDataType("boolean")
                    .setColumnName("doc['truth']")
                    .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("truth")))
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

            var infos1 = InformationSchemaLoader.withTableName("test").load(conn);
            assertThat(infos1.isEmpty()).isFalse();
            for (InformationSchemaColumnInfo info : infos1) {
                LOGGER.info("{}", info);
            }

            ColumnTypeManager manager1 = new ColumnTypeManager();
            manager1.fromInformationSchema(infos1);
            manager1.print();

            ObjectMapper mapper = new ObjectMapper();
            List<Object> generated = new ArrayList<>();

            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test(id, doc) VALUES (?, ?)")) {
                for (int i = 0; i < 10; i++) {
                    Object object = generateObject();
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

            var infos2 = InformationSchemaLoader.withTableName("test").load(conn);
            assertThat(infos2.isEmpty()).isFalse();

            for (InformationSchemaColumnInfo info : infos2) {
                LOGGER.info("{}", info);
            }

            ColumnTypeManager manager2 = new ColumnTypeManager();
            manager2.fromInformationSchema(infos2);
            manager2.print();
        });
    }

    private Object generateObject() {
        return Map.of(
                "id", 4,
                "role", "King",
                generateKey(), generateValue(),
                generateRandonChar(), -123.31239);
    }

    private Object generateValue() {
        // random value
        var rand = Math.random();
        if (rand < 0.20) {
            return "Queen";
        }
        else if (rand < 0.40) {
            return 666;
        }
        else if (rand < 0.60) {
            return true;
        }
        else if (rand < 0.80) {
            return generateList();
        }
        else {
            return Map.of(
                    "truth", false,
                    "lucky", 444);
        }
    }

    private String generateKey() {
        var rand = Math.random();
        if (rand < 0.25) {
            return "name";
        }
        else if (rand < 0.50) {
            return "name_" + generateShortTypeName();
        }
        else if (rand < 0.75) {
            return "name_" + generateRandonChar();
        }
        else {
            return generateShortTypeName();
        }
    }

    private static List<String> randomChar = List.of("" +
            "[", ",", ".", "!", "@", "#", "$", "%", "^", "&", "*", "(", ")",
            "-", "+", "=", "{", "}", "[", "]", "|", "\\",
            ";", ":", "'", "\"", "<", ">", "?", "/", "~", "`");

    private String generateRandonChar() {
        return randomChar.get((int) (Math.random() * randomChar.size()));
    }

    private static List<String> shortTypeNames = List.of(
            "smallint", "bigint", "integer",
            "double precision", "real",
            "timestamp with time zone", "timestamp without time zone",
            "bit",
            "ip", "text",
            "object", "boolean", "character", "float_vector",
            "geo_point", "geo_shape");

    private String generateShortTypeName() {
        return shortTypeNames.get((int) (Math.random() * shortTypeNames.size()));
    }

    private List<Object> generateList() {
        var rand = Math.random();
        if (rand < 0.33) {
            return List.of(
                    generateValue(),
                    generateValue(),
                    generateValue());
        }
        else if (rand < 0.66) {
            var val = generateValue();
            return List.of(val, val);
        }
        else {
            return List.of();
        }
    }
}
