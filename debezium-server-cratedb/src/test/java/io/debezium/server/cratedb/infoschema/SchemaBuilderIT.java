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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.server.cratedb.CrateTestResourceLifecycleManager;
import io.debezium.server.cratedb.Profile;
import io.debezium.server.cratedb.datagen.DataGen;
import io.debezium.server.cratedb.schema.CrateSQL;
import io.debezium.server.cratedb.schema.Evolution;
import io.debezium.server.cratedb.schema.Schema;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(Profile.PostgresAndCrateDB.class)
class SchemaBuilderIT {
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
                            "truth", Schema.Primitive.BOOLEAN)));
        });
    }

    @Test
    void withGenerated() {
        // set randomness seed
        // working = 123456789 - rebuild collision type on first level
        // working = 123456791 - rebuild collision at nested level
        // working = 123456788 - Mixed dataTypes inside a list are not supported. Found integer and object
        // working = 1727213537780L - ERROR: Dynamic nested arrays are not supported
        // working = 1727306306043L ERROR: Mixed dataTypes inside a list are not supported. Found boolean and object
        // working - 1727390353003L - similarity assertion, object_array reconstruction
        var seed = System.currentTimeMillis();
        LOGGER.error("seed={}", seed);
        DataGen.setSeed(seed);

        assertDoesNotThrow(() -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS test");
                stmt.execute("CREATE TABLE test (id TEXT PRIMARY KEY, doc OBJECT)");
                stmt.execute("REFRESH TABLE test");
            }

            var infos1 = DataLoader.withTableName("test").load(conn);
            assertThat(infos1.isEmpty()).isFalse();
            for (ColumnInfo info : infos1) {
                LOGGER.info("info1:{}", info);
            }

            var manager0 = SchemaBuilder.fromInformationSchema(infos1);
            LOGGER.info("manager1:{}", manager0);

            var inner = Evolution.fromPath(List.of("doc"), manager0);
            assertThat(inner.isEmpty()).isFalse();

            var manager1 = inner.get();

            ObjectMapper mapper = new ObjectMapper();
            List<Object> generated = new ArrayList<>();

            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test(id, doc) VALUES (?, ?)")) {
                for (int i = 0; i < 10; i++) {
                    Object object = DataGen.generateObject();
                    generated.add(object);

                    LOGGER.error("BEFORE object0: {}", object);
                    LOGGER.info("BEFORE manager1:{}", manager1);

                    var result1 = Evolution.fromObject(manager1, object);
                    var schema1 = result1.getLeft();
                    var object1 = result1.getRight();
                    LOGGER.info("BEFORE schema1: {}", schema1);
                    LOGGER.error("BEFORE object1: {}", object1);
                    // var object2 = Evolution.sanitizeData(schema1, object1);
                    // LOGGER.error("BEFORE object2: {}", object2);
                    // var alters = CrateSQL.toSQL("test", manager1, schema1);
                    var alters = CrateSQL.toSQL("test",
                            Schema.Dict.of("doc", manager1),
                            Schema.Dict.of("doc", schema1));

                    manager1 = schema1;

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

                    String json = mapper.writeValueAsString(object1);

                    LOGGER.error("POST: {}", json);
                    stmt.setString(1, String.valueOf(i));
                    stmt.setString(2, json);
                    try {
                        stmt.execute();
                    }
                    catch (Exception e) {
                        LOGGER.error("BEFORE EXCEPTION:");
                        LOGGER.info("manager1={}", manager1);
                        throw e;
                    }
                }
            }

            LOGGER.info("Generated: {}", generated);
            LOGGER.info("manager1={}", manager1);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("REFRESH TABLE test");
            }

            var infos2 = DataLoader.withTableName("test").load(conn);
            assertThat(infos2.isEmpty()).isFalse();

            for (ColumnInfo info : infos2) {
                LOGGER.info("info2:{}", info);
            }

            var manager2 = SchemaBuilder.fromInformationSchema(infos2);
            var result = Evolution.fromPath(List.of("doc"), manager2);
            assertThat(result.isEmpty()).isFalse();
            var manager3 = result.get();

            LOGGER.info("manager0={}", manager0);
            LOGGER.info("manager1={}", manager1);
            LOGGER.info("manager2={}", manager2);
            LOGGER.info("manager3={}", manager3);

            assertThat(Evolution.similar(manager1, manager3)).isTrue();
            // assertThat(manager1).
            // usingRecursiveComparison().
            // isEqualTo(manager3);
        });
    }
}
