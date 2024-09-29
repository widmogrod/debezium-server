/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.server.DebeziumServer;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgresSQL database and writing to CrateDB stream.
 *
 * @author Gabriel habryn
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(CrateTestResourceLifecycleManager.class)
public class CrateDBIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrateDBIT.class);

    private static Connection conn;

    @Inject
    DebeziumServer server;

    {
        Testing.Files.delete(CrateDBTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(CrateDBTestConfigSource.OFFSET_STORE_PATH);
    }

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

    void setupDependencies(@Observes ConnectorStartedEvent event) throws Exception {
        conn = DriverManager.getConnection(CrateTestResourceLifecycleManager.getUrl());
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
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
                // .containsEntry("file", CrateDBTestConfigSource.TEST_FILE_PATH.toString())
                // .containsEntry("offset.storage.file.filename", CrateDBTestConfigSource.OFFSET_STORE_PATH.toString())
                .containsEntry("schema.include.list", "inventory")
                .containsEntry("table.include.list", "inventory.customers, inventory.cratedb_test")
                .containsEntry("transforms", "addheader")
                // .containsEntry("transforms.hoist.type", "org.apache.kafka.connect.transforms.HoistField$Value")
                // .containsEntry("transforms.hoist.field", "payload")
                .containsEntry("offset.storage.cratedb.connection_url", CrateTestResourceLifecycleManager.getUrl());
    }

    @Test
    void testPostgresDefaultState() throws SQLException, JsonProcessingException {
        Testing.Print.enable();

        List<Map<String, Object>> expectedResults = List.of(
                Map.of("id", "\"1001\"", "doc", Map.of("last_name", "Thomas", "id", 1001, "first_name", "Sally", "email", "sally.thomas@acme.com")),
                Map.of("id", "\"1002\"", "doc", Map.of("last_name", "Bailey", "id", 1002, "first_name", "George", "email", "gbailey@foobar.com")),
                Map.of("id", "\"1003\"", "doc", Map.of("last_name", "Walker", "id", 1003, "first_name", "Edward", "email", "ed@walker.com")),
                Map.of("id", "\"1004\"", "doc", Map.of("last_name", "Kretchmar", "id", 1004, "first_name", "Anne", "email", "annek@noanswer.org")));

        Statement stmt = conn.createStatement();

        Awaitility.await().atMost(Duration.ofSeconds(CrateDBTestConfigSource.waitForSeconds())).until(() -> {
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
    void testPostgresCustomTableAndInsertsAndDeletes() throws SQLException, JsonProcessingException {
        Testing.Print.enable();

        Awaitility.await().atMost(Duration.ofSeconds(CrateDBTestConfigSource.waitForSeconds())).until(() -> {
            // NOTE: Running this code inside await ensures that Debezium sees those changes as CDC events, and not as snapshot (reads)
            // which makes more accurate representation of how CrateDB state will look after all those operations
            final PostgresConnection connection = PostgresTestResourceLifecycleManager.getPostgresConnection();
            // data manipulation
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
            Awaitility.await().atMost(Duration.ofSeconds(CrateDBTestConfigSource.waitForSeconds())).until(() -> {
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
                            put("id", "\"2\"");
                            put("doc", new HashMap<>() {
                                {
                                    put("descr", "hello 2");
                                    put("id", 2);
                                }
                            });
                        }
                    });
                    add(new HashMap<>() {
                        {
                            put("id", "\"3\"");
                            put("doc", new HashMap<>() {
                                {
                                    put("descr", "hello 33");
                                    put("id", 3);
                                }
                            });
                        }
                    });
                    add(new HashMap<>() {
                        {
                            put("id", "\"4\"");
                            put("doc", new HashMap<>() {
                                {
                                    put("descr", "hello 4");
                                    put("id", 4);
                                    put("new_col", "new data");
                                }
                            });
                        }
                    });
                    add(new HashMap<>() {
                        {
                            put("id", "\"5\"");
                            put("doc", new HashMap<>() {
                                {
                                    put("descr", "hello 5");
                                    put("id", 5);
                                    put("new_col", "new description ");
                                }
                            });
                        }
                    });
                    add(new HashMap<>() {
                        {
                            put("id", "\"7\"");
                            put("doc", new HashMap<>() {
                                {
                                    put("id", 7);
                                    put("new_col", "7");
                                }
                            });
                        }
                    });
                    add(new HashMap<>() {
                        {
                            put("id", "\"8\"");
                            put("doc", new HashMap<>() {
                                {
                                    put("id", 8);
                                    put("new_col", "[1.1, 2.2, 3.3]");
                                }
                            });
                        }
                    });
                }
            };

            assertThat(results).usingRecursiveComparison().isEqualTo(expectedResults);

            return true;
        });
    }
}
