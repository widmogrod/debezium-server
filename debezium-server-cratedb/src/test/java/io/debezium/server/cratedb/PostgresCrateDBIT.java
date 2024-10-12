/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import static io.debezium.server.cratedb.Profile.DEBEZIUM_SOURCE_MONGODB_CONNECTION_STRING;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import jakarta.inject.Inject;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.server.DebeziumServer;
import io.debezium.util.Testing;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration test that verifies basic reading from PostgresSQL database and writing to CrateDB stream.
 *
 * @author Gabriel Habryn
 */
@QuarkusTest
@TestProfile(Profile.PostgresAndCrateDB.class)
@DisabledIfEnvironmentVariable(named = DEBEZIUM_SOURCE_MONGODB_CONNECTION_STRING, matches = ".*", disabledReason = "Quarkus has some issue, when MongoDB and Postgres are run together")
public class PostgresCrateDBIT {
    private static Connection conn;

    @Inject
    DebeziumServer server;

    @BeforeEach
    void setup() throws Exception {
        // Initialize the connection
        conn = DriverManager.getConnection(CrateTestResourceLifecycleManager.getUrl());
    }

    @AfterEach
    void tearDown() throws Exception {
        // Close the connection if it's not null and not closed
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    @Test
    void testConfigIsCorrect() {
        // This is mostly sanity check, if by any chance core or upstream component
        // didn't change configuration.
        // Also, this section can help to document what are configuration settings necessary for connector to work.
        var props = server.getProps();
        assertThat(props).containsEntry("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .containsEntry("name", "cratedb")
                .containsEntry("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .containsEntry("schema.include.list", "inventory")
                .containsEntry("table.include.list", "inventory.customers, inventory.cratedb_test")
                .containsEntry("transforms", "unwrap")
                .containsEntry("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
                .containsEntry("transforms.unwrap.add.headers", "op")
                .containsEntry("transforms.unwrap.drop.tombstones", "false")
                .containsEntry("offset.storage.cratedb.type_conflict_strategy", "malformed")
                .containsEntry("offset.storage.cratedb.connection_url", CrateTestResourceLifecycleManager.getUrl());
    }

    @Test
    void testPostgresDefaultState() throws SQLException, JsonProcessingException {
        Testing.Print.enable();

        List<Map<String, Object>> expectedResults = List.of(
                Map.of("id", "1001", "doc", Map.of("last_name", "Thomas", "id", 1001, "first_name", "Sally", "email", "sally.thomas@acme.com")),
                Map.of("id", "1002", "doc", Map.of("last_name", "Bailey", "id", 1002, "first_name", "George", "email", "gbailey@foobar.com")),
                Map.of("id", "1003", "doc", Map.of("last_name", "Walker", "id", 1003, "first_name", "Edward", "email", "ed@walker.com")),
                Map.of("id", "1004", "doc", Map.of("last_name", "Kretchmar", "id", 1004, "first_name", "Anne", "email", "annek@noanswer.org")));

        Statement stmt = conn.createStatement();

        Awaitility.await().atMost(Duration.ofSeconds(Profile.waitForSeconds())).until(() -> {
            try {
                stmt.execute("REFRESH TABLE testc_inventory_customers;");
                var result = stmt.executeQuery("SELECT COUNT(1) FROM testc_inventory_customers");
                result.next();
                return result.getInt(1) == expectedResults.size();
            }
            catch (SQLException e) {
                return false;
            }
        });

        // Collect table representation
        List<Object> results = new ArrayList<>();
        ResultSet itemsSet = stmt.executeQuery("SELECT id, doc FROM testc_inventory_customers ORDER BY id ASC;");
        while (itemsSet.next()) {
            String id = itemsSet.getString(1);
            String docJson = itemsSet.getString(2);
            Map doc = new ObjectMapper().readValue(docJson, Map.class);
            results.add(Map.of("id", id, "doc", doc));
        }
        itemsSet.close();

        // Make sure we have expected state
        assertThat(results).usingRecursiveComparison().isEqualTo(expectedResults);
    }

    @Test
    void testPostgresCustomTableAndInsertsAndDeletes() {
        Testing.Print.enable();

        Awaitility.await().atMost(Duration.ofSeconds(Profile.waitForSeconds())).until(() -> {
            // NOTE: Running this code inside await ensures that Debezium sees those changes as CDC events, and not as snapshot (reads)
            // which makes more accurate representation of how CrateDB state will look after all those operations
            final PostgresConnection connection = PostgresTestResourceLifecycleManager.getPostgresConnection();
            // data manipulation
            connection.execute("DROP TABLE IF EXISTS inventory.cratedb_test;");
            connection.execute("CREATE TABLE inventory.cratedb_test (id INT PRIMARY KEY, descr TEXT)");
            connection.execute("INSERT INTO inventory.cratedb_test VALUES (1, 'hello 1')");
            connection.execute("INSERT INTO inventory.cratedb_test VALUES (2, 'hello 2')");
            connection.execute("INSERT INTO inventory.cratedb_test VALUES (3, 'hello 3')");
            connection.execute("DELETE FROM inventory.cratedb_test WHERE id=1");
            connection.execute("UPDATE inventory.cratedb_test SET descr = 'hello 33' WHERE id=3");
            // schema changes, let's try to mess up data
            // 1. we add new column
            connection.execute("ALTER TABLE inventory.cratedb_test ADD COLUMN new_col TEXT DEFAULT 'new description '");
            connection.execute("INSERT INTO inventory.cratedb_test (id, descr, new_col) VALUES (4, 'hello 4', 'new data')");
            connection.execute("INSERT INTO inventory.cratedb_test (id, descr) VALUES (5, 'hello 5')");
            // 4. let's change type of new_col from string to int
            connection.execute("ALTER TABLE inventory.cratedb_test ALTER COLUMN new_col DROP DEFAULT;");
            connection
                    .execute("ALTER TABLE inventory.cratedb_test ALTER COLUMN new_col TYPE INT USING CASE WHEN new_col ~ '^[0-9]+$' THEN new_col::integer ELSE NULL END");
            connection.execute("INSERT INTO inventory.cratedb_test (id, new_col) VALUES (7, 7)");
            // 6. let's change new_col to FLOAT[]
            connection.execute("""
                    ALTER TABLE inventory.cratedb_test ALTER COLUMN new_col TYPE FLOAT[] USING
                             CASE
                                 WHEN new_col IS NOT NULL THEN ARRAY[new_col::float]  -- Convert existing non-null values into float arrays
                                 ELSE NULL  -- Set NULL for invalid or null data
                             END;
                    """);
            connection.execute("INSERT INTO inventory.cratedb_test (id, new_col) VALUES (8, '{1.1, 2.2, 3.3}')");

            // Assert postgresql final state is as expected
            List<Object> resultPostgres = new ArrayList<>();
            connection.query("SELECT * FROM inventory.cratedb_test", new JdbcConnection.ResultSetConsumer() {
                @Override
                public void accept(ResultSet rs) throws SQLException {
                    // Gather all the table data
                    while (rs.next()) {
                        var id = rs.getInt("id");
                        var descr = rs.getString("descr");
                        var newColArray = rs.getArray("new_col");
                        List<Float> newCol = null;
                        if (newColArray != null) {
                            Double[] doubleArray = (Double[]) newColArray.getArray();
                            newCol = Arrays.stream(doubleArray)
                                    .map(Double::floatValue)
                                    .collect(Collectors.toList());
                        }

                        var record = new HashMap<String, Object>();
                        record.put("id", id);
                        record.put("descr", descr);
                        record.put("new_col", newCol);
                        resultPostgres.add(record);
                    }
                }
            });
            connection.close();

            // And finally check the state
            var expectedPostgresState = Arrays.asList(
                    new HashMap<String, Object>() {
                        {
                            put("descr", "hello 2");
                            put("id", 2);
                            put("new_col", null);
                        }
                    },
                    new HashMap<String, Object>() {
                        {
                            put("descr", "hello 33");
                            put("id", 3);
                            put("new_col", null);
                        }
                    },
                    new HashMap<String, Object>() {
                        {
                            put("descr", "hello 4");
                            put("id", 4);
                            put("new_col", null);
                        }
                    },
                    new HashMap<String, Object>() {
                        {
                            put("descr", "hello 5");
                            put("id", 5);
                            put("new_col", null);
                        }
                    },
                    new HashMap<String, Object>() {
                        {
                            put("descr", null);
                            put("id", 7);
                            put("new_col", List.of(7f));
                        }
                    },
                    new HashMap<String, Object>() {
                        {
                            put("descr", null);
                            put("id", 8);
                            put("new_col", List.of(1.1f, 2.2f, 3.3f));
                        }
                    });
            assertThat(resultPostgres).usingRecursiveAssertion().isEqualTo(expectedPostgresState);

            // Let's proceed with CrateDB
            Statement stmt = conn.createStatement();

            // Let's give some time to propagate the changes
            // First let's refresh the table that should be created by consumer
            // IF this step fails, most likely, consumer haven't process any events
            Awaitility.await().atMost(Duration.ofSeconds(Profile.waitForSeconds())).until(() -> {
                try {
                    stmt.execute("REFRESH TABLE testc_inventory_cratedb_test;");
                    var result = stmt.executeQuery("SELECT COUNT(1) FROM testc_inventory_cratedb_test");
                    result.next();
                    return result.getInt(1) == expectedPostgresState.size();
                }
                catch (SQLException e) {
                    return false;
                }
            });

            List<Object> results = new ArrayList<>();
            ResultSet itemsSet = stmt.executeQuery("SELECT id, doc, malformed, err FROM testc_inventory_cratedb_test ORDER BY id ASC;");
            while (itemsSet.next()) {
                String id = itemsSet.getString(1);
                String docJson = itemsSet.getString(2);
                String malformedJson = itemsSet.getString(3);
                String errText = itemsSet.getString(4);
                // prepare data
                var doc = new ObjectMapper().readValue(docJson, Map.class);
                var malformed = malformedJson == null ? null : new ObjectMapper().readValue(malformedJson, Map.class);
                // build final representation
                results.add(
                        new HashMap<>() {{
                            put("id", id);
                            put("doc", doc);
                            put("malformed", (malformed != null && malformed.isEmpty()) ? null : malformed);
                            put("err", errText);
                        }}
                );
            }
            itemsSet.close();

            List<HashMap<String, Object>> expectedResults = new ArrayList<>() {
                {
                    add(new HashMap<>() {
                        {
                            put("id", "2");
                            put("doc", new HashMap<>() {
                                {
                                    put("descr", "hello 2");
                                    put("id", 2);
                                }
                            });
                            put("malformed", null);
                            put("err", null);
                        }
                    });
                    add(new HashMap<>() {
                        {
                            put("id", "3");
                            put("doc", new HashMap<>() {
                                {
                                    put("descr", "hello 33");
                                    put("id", 3);
                                }
                            });
                            put("malformed", null);
                            put("err", null);
                        }
                    });
                    add(new HashMap<>() {
                        {
                            put("id", "4");
                            put("doc", new HashMap<>() {
                                {
                                    put("descr", "hello 4");
                                    put("id", 4);
                                    put("new_col", "new data");
                                }
                            });
                            put("malformed", null);
                            put("err", null);
                        }
                    });
                    add(new HashMap<>() {
                        {
                            put("id", "5");
                            put("doc", new HashMap<>() {
                                {
                                    put("descr", "hello 5");
                                    put("id", 5);
                                    put("new_col", "new description ");
                                }
                            });
                            put("malformed", null);
                            put("err", null);
                        }
                    });
                    add(new HashMap<>() {
                        {
                            put("id", "7");
                            put("doc", new HashMap<>() {
                                {
                                    put("id", 7);
                                    put("new_col", "7");
                                }
                            });
                            put("malformed", Map.of(
                                    "new_col", 7
                            ));
                            put("err", null);
                        }
                    });
                    add(new HashMap<>() {
                        {
                            put("id", "8");
                            put("doc", new HashMap<>() {
                                {
                                    put("id", 8);
                                    put("new_col", "[1.1, 2.2, 3.3]");
                                }
                            });
                            put("malformed",  Map.of(
                                    "new_col", List.of(1.1, 2.2, 3.3)
                            ));
                            put("err", null);
                        }
                    });
                }
            };

            assertThat(results).usingRecursiveComparison().isEqualTo(expectedResults);

            return true;
        });
    }

    @Test
    void testAllPossiblePSQLTypes() {
        Testing.Print.enable();

        Awaitility.await().atMost(Duration.ofSeconds(Profile.waitForSeconds())).until(() -> {
            final PostgresConnection connection = PostgresTestResourceLifecycleManager.getPostgresConnection();
            // data manipulation
            connection.execute("CREATE EXTENSION IF NOT EXISTS hstore;");
            connection.execute("DROP TABLE IF EXISTS inventory.cratedb_test;");
            connection.execute("""
                    CREATE TABLE inventory.cratedb_test (
                        serial_id           SERIAL PRIMARY KEY,                 -- Auto-incrementing integer
                        small_int_value     SMALLINT,                           -- Small integer (-32768 to +32767)
                        int_value           INTEGER,                            -- Standard integer (-2147483648 to +2147483647)
                        big_int_value       BIGINT,                             -- Large integer (-9223372036854775808 to 9223372036854775807)
                        decimal_value       DECIMAL(10, 5),                     -- Decimal type with precision
                        numeric_value       NUMERIC(10, 2),                     -- Numeric type with precision and scale
                        real_value          REAL,                               -- Floating-point number (4 bytes)
                        double_value        DOUBLE PRECISION,                   -- Floating-point number (8 bytes)
                        small_serial        SMALLSERIAL,                        -- Auto-incrementing small integer
                        serial_value        SERIAL,                             -- Auto-incrementing integer
                        big_serial_value    BIGSERIAL,                          -- Auto-incrementing big integer
                        char_value          CHAR(5),                            -- Fixed-length character string
                        varchar_value       VARCHAR(255),                       -- Variable-length character string
                        text_value          TEXT,                               -- Variable-length unlimited string
                        boolean_value       BOOLEAN,                            -- Boolean (true/false)
                        date_value          DATE,                               -- Date type
                        time_value          TIME,                               -- Time of day (no time zone)
                        timestamp_value     TIMESTAMP,                          -- Both date and time (no time zone)
                        timestamptz_value   TIMESTAMPTZ,                        -- Timestamp with time zone
                        interval_value      INTERVAL,                           -- Time span
                        uuid_value          UUID,                               -- Universally Unique Identifier
                        json_value          JSON,                               -- JSON data type
                        jsonb_value         JSONB,                              -- Binary JSON data type
                        xml_value           XML,                                -- XML data type
                        array_value         INT[],                              -- Array of integers
                        hstore_value        HSTORE,                             -- Key-value store
                        inet_value          INET,                               -- IP addresses
                        cidr_value          CIDR,                               -- Network IP address
                        macaddr_value       MACADDR,                            -- MAC addresses
                        bit_value           BIT(4),                             -- Bit-string, fixed length
                        varbit_value        VARBIT(10),                         -- Bit-string, variable length
                        point_value         POINT,                              -- Geometric point
                        line_value          LINE,                               -- Geometric line
                        lseg_value          LSEG,                               -- Line segment
                        box_value           BOX,                                -- Rectangular box
                        path_value          PATH,                               -- Geometric path
                        polygon_value       POLYGON,                            -- Polygon
                        circle_value        CIRCLE                              -- Geometric circle
                    );
                    """);
            connection.execute("""
                    INSERT INTO inventory.cratedb_test (
                        small_int_value,
                        int_value,
                        big_int_value,
                        decimal_value,
                        numeric_value,
                        real_value,
                        double_value,
                        small_serial,
                        serial_value,
                        big_serial_value,
                        char_value,
                        varchar_value,
                        text_value,
                        boolean_value,
                        date_value,
                        time_value,
                        timestamp_value,
                        timestamptz_value,
                        interval_value,
                        uuid_value,
                        json_value,
                        jsonb_value,
                        xml_value,
                        array_value,
                        hstore_value,
                        inet_value,
                        cidr_value,
                        macaddr_value,
                        bit_value,
                        varbit_value,
                        point_value,
                        line_value,
                        lseg_value,
                        box_value,
                        path_value,
                        polygon_value,
                        circle_value
                    ) VALUES (
                        32767,                                                -- small_int_value
                        2147483647,                                           -- int_value
                        9223372036854775807,                                  -- big_int_value
                        12345.67890,                                          -- decimal_value
                        12345.67,                                             -- numeric_value
                        1.2345,                                               -- real_value
                        123456789.12345678,                                   -- double_value
                        DEFAULT,                                              -- small_serial (auto-generated)
                        DEFAULT,                                              -- serial_value (auto-generated)
                        DEFAULT,                                              -- big_serial_value (auto-generated)
                        'abcde',                                              -- char_value
                        'example varchar',                                    -- varchar_value
                        'This is a text value',                               -- text_value
                        TRUE,                                                 -- boolean_value
                        '2024-01-01',                                         -- date_value
                        '12:34:56',                                           -- time_value
                        '2024-01-01 12:34:56',                                -- timestamp_value
                        '2024-01-01 12:34:56+00',                             -- timestamptz_value
                        '1 year 2 months 3 days',                             -- interval_value
                        '123e4567-e89b-12d3-a456-426614174000',               -- uuid_value
                        '{"key": "value"}',                                   -- json_value
                        '{"key": "value"}',                                   -- jsonb_value
                        '<tag>Example XML</tag>',                             -- xml_value
                        '{1, 2, 3}',                                          -- array_value
                        '"key"=>"value"',                                     -- hstore_value
                        '192.168.1.1',                                        -- inet_value
                        '192.168.100.128/25',                                 -- cidr_value
                        '08:00:2b:01:02:03',                                  -- macaddr_value
                        B'1010',                                              -- bit_value
                        B'101010',                                            -- varbit_value
                        '(1.0, 2.0)',                                         -- point_value
                        '{1,1,-1}',                                          -- line_value
                        '[(0, 0), (1, 1)]',                                   -- lseg_value
                        '(1,1),(0,0)',                                        -- box_value
                        '[(0, 0), (1, 1), (1, 0)]',                           -- path_value
                        '((0, 0), (1, 1), (1, 0), (0, 0))',                   -- polygon_value
                        '<(0, 0), 5>'                                         -- circle_value
                    );
                    """);

            // Assert postgresql final state is as expected
            List<Object> resultPostgres = new ArrayList<>();
            connection.query("SELECT * FROM inventory.cratedb_test", new JdbcConnection.ResultSetConsumer() {
                @Override
                public void accept(ResultSet rs) throws SQLException {
                    // Gather all the table data
                    while (rs.next()) {
                        var data = new HashMap<String, Object>();
                        data.put("serial_id", rs.getInt("serial_id"));
                        data.put("small_serial", rs.getInt("small_serial"));
                        data.put("serial_value", rs.getInt("serial_value"));
                        data.put("big_serial_value", rs.getInt("big_serial_value"));
                        data.put("small_int_value", rs.getShort("small_int_value"));
                        data.put("int_value", rs.getInt("int_value"));
                        data.put("big_int_value", rs.getLong("big_int_value"));
                        data.put("decimal_value", rs.getBigDecimal("decimal_value"));
                        data.put("numeric_value", rs.getBigDecimal("numeric_value"));
                        data.put("real_value", rs.getFloat("real_value"));
                        data.put("double_value", rs.getDouble("double_value"));
                        data.put("char_value", rs.getString("char_value"));
                        data.put("varchar_value", rs.getString("varchar_value"));
                        data.put("text_value", rs.getString("text_value"));
                        data.put("boolean_value", rs.getBoolean("boolean_value"));
                        data.put("date_value", rs.getDate("date_value").toLocalDate());
                        data.put("time_value", rs.getTime("time_value"));
                        data.put("timestamp_value", rs.getTimestamp("timestamp_value").toLocalDateTime());
                        data.put("timestamptz_value", rs.getObject("timestamptz_value", OffsetDateTime.class));
                        data.put("interval_value", rs.getString("interval_value"));
                        data.put("uuid_value", (UUID) rs.getObject("uuid_value"));
                        data.put("json_value", rs.getString("json_value"));
                        data.put("jsonb_value", rs.getString("jsonb_value"));
                        data.put("xml_value", rs.getString("xml_value"));
                        data.put("array_value", (Integer[]) rs.getArray("array_value").getArray());
                        data.put("hstore_value", rs.getString("hstore_value"));
                        data.put("inet_value", rs.getString("inet_value"));
                        data.put("cidr_value", rs.getString("cidr_value"));
                        data.put("macaddr_value", rs.getString("macaddr_value"));
                        data.put("bit_value", rs.getString("bit_value"));
                        data.put("varbit_value", rs.getString("varbit_value"));
                        data.put("point_value", rs.getString("point_value"));
                        data.put("line_value", rs.getString("line_value"));
                        data.put("lseg_value", rs.getString("lseg_value"));
                        data.put("box_value", rs.getString("box_value"));
                        data.put("path_value", rs.getString("path_value"));
                        data.put("polygon_value", rs.getString("polygon_value"));
                        data.put("circle_value", rs.getString("circle_value"));
                        resultPostgres.add(data);
                    }
                }
            });
            connection.close();

            // And finally check the state
            var expectedPostgresState = Arrays.asList(
                    new HashMap<String, Object>() {
                        {
                            put("serial_id", 1);
                            put("small_int_value", (short) 32767);
                            put("int_value", 2147483647);
                            put("big_int_value", 9223372036854775807L);
                            put("decimal_value", new BigDecimal("12345.67890"));
                            put("numeric_value", new BigDecimal("12345.67"));
                            put("real_value", 1.2345f);
                            put("double_value", 1.2345678912345678E8);
                            put("small_serial", 1);
                            put("serial_value", 1);
                            put("big_serial_value", 1);
                            put("char_value", "abcde");
                            put("varchar_value", "example varchar");
                            put("text_value", "This is a text value");
                            put("boolean_value", true);
                            put("date_value", LocalDate.of(2024, 1, 1));
                            put("time_value", Time.valueOf("12:34:56"));
                            put("timestamp_value", LocalDateTime.of(2024, 1, 1, 12, 34, 56));
                            put("timestamptz_value", OffsetDateTime.of(2024, 1, 1, 12, 34, 56, 0, ZoneOffset.UTC));
                            put("interval_value", "1 year 2 mons 3 days");
                            put("uuid_value", UUID.fromString("123e4567-e89b-12d3-a456-426614174000"));
                            put("json_value", "{\"key\": \"value\"}");
                            put("jsonb_value", "{\"key\": \"value\"}");
                            put("xml_value", "<tag>Example XML</tag>");
                            put("array_value", new Integer[]{ 1, 2, 3 });
                            put("hstore_value", "\"key\"=>\"value\"");
                            put("inet_value", "192.168.1.1");
                            put("cidr_value", "192.168.100.128/25");
                            put("macaddr_value", "08:00:2b:01:02:03");
                            put("bit_value", "1010");
                            put("varbit_value", "101010");
                            put("point_value", "(1,2)");
                            put("line_value", "{1,1,-1}");
                            put("lseg_value", "[(0,0),(1,1)]");
                            put("box_value", "(1,1),(0,0)");
                            put("path_value", "[(0,0),(1,1),(1,0)]");
                            put("polygon_value", "((0,0),(1,1),(1,0),(0,0))");
                            put("circle_value", "<(0,0),5>");
                        }
                    });
            assertThat(resultPostgres).usingRecursiveComparison().isEqualTo(expectedPostgresState);

            // Let's proceed with CrateDB
            Statement stmt = conn.createStatement();

            // Let's give some time to propagate the changes
            // First let's refresh the table that should be created by consumer
            // IF this step fails, most likely, consumer haven't process any events
            Awaitility.await().atMost(Duration.ofSeconds(Profile.waitForSeconds())).until(() -> {
                try {
                    stmt.execute("REFRESH TABLE testc_inventory_cratedb_test;");
                    var result = stmt.executeQuery("SELECT COUNT(1) FROM testc_inventory_cratedb_test");
                    result.next();
                    return result.getInt(1) == expectedPostgresState.size();
                }
                catch (SQLException e) {
                    return false;
                }
            });

            List<Object> results = new ArrayList<>();
            ResultSet itemsSet = stmt.executeQuery("SELECT id, doc FROM testc_inventory_cratedb_test ORDER BY id ASC;");
            while (itemsSet.next()) {
                String id = itemsSet.getString(1);
                String docJson = itemsSet.getString(2);
                Map doc = new ObjectMapper().readValue(docJson, Map.class);
                results.add(Map.of("id", id, "doc", doc));
            }
            itemsSet.close();

            // TODO: figure out how to include schema changes
            // current Debezium settings don't stream schema changes
            // and CrateDB don't have some of the operations like change type of a column
            List<HashMap<String, Object>> expectedResults = new ArrayList<>() {
                {
                    add(new HashMap<>() {
                        {
                            put("id", "1");
                            put("doc", new HashMap<String, Object>() {
                                {
                                    put("serial_id", 1);
                                    put("small_int_value", 32767);
                                    put("int_value", 2147483647);
                                    put("big_int_value", 9223372036854775807L);
                                    put("decimal_value", "SZYC0g=="); // FIXME https://debezium.io/documentation/faq/#how_to_retrieve_decimal_field_from_binary_representation
                                    put("numeric_value", "EtaH");
                                    put("real_value", 1.2345);
                                    put("double_value", 1.2345678912345678E8);
                                    put("small_serial", 1);
                                    put("serial_value", 1);
                                    put("big_serial_value", 1);
                                    put("char_value", "abcde");
                                    put("varchar_value", "example varchar");
                                    put("text_value", "This is a text value");
                                    put("boolean_value", true);
                                    put("date_value", 19723); // number of days since the epoch
                                    put("time_value", 45296000000L); // number of milliseconds past midnight w/o tz,
                                    put("timestamp_value", 1704112496000000L);
                                    put("timestamptz_value", "2024-01-01T12:34:56.000000Z");
                                    put("interval_value", 37076400000000L); // debezium interpretation: The approximate number of microseconds for a time interval using the 365.25 / 12.0 formula for days per month average.
                                    put("uuid_value", "123e4567-e89b-12d3-a456-426614174000");
                                    put("json_value", "{\"key\": \"value\"}");
                                    put("jsonb_value", "{\"key\": \"value\"}");
                                    put("xml_value", "<tag>Example XML</tag>");
                                    put("array_value", new ArrayList(List.of(1, 2, 3)));
                                    put("hstore_value", "{\"key\":\"value\"}");
                                    put("inet_value", "192.168.1.1");
                                    put("cidr_value", "192.168.100.128/25");
                                    put("macaddr_value", "08:00:2b:01:02:03");
                                    put("bit_value", "Cg==");
                                    put("varbit_value", "Kg==");
                                    put("point_value", new HashMap<String, Object>() {
                                        {
                                            put("wkb", "AQEAAAAAAAAAAADwPwAAAAAAAABA");
                                            put("x", 1.0);
                                            put("y", 2.0);
                                        }
                                    });

                                    // NOTE: TableSchemaBuilder: Unexpected JDBC type '1111' for column 'line_value' that will be ignored
                                    // put("point_value", "(1,2)");
                                    // put("line_value", "{1,1,-1}");
                                    // put("lseg_value", "[(0,0),(1,1)]");
                                    // put("box_value", "(1,1),(0,0)");
                                    // put("path_value", "[(0,0),(1,1),(1,0)]");
                                    // put("polygon_value", "((0,0),(1,1),(1,0),(0,0))");
                                    // put("circle_value", "<(0,0),5>");
                                }
                            });
                        }
                    });
                }
            };

            assertThat(results).isEqualTo(expectedResults);

            return true;
        });
    }
}
