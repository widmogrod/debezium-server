/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.server.DebeziumServer;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.server.events.TaskStartedEvent;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to CrateDB stream.
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

    void taskStarted(@Observes TaskStartedEvent event) throws Exception {
        LOGGER.info("Starting task {}", event);
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
    void testCrateDB() {
        Testing.Print.enable();

        Awaitility.await().atMost(Duration.ofSeconds(CrateDBTestConfigSource.waitForSeconds())).until(() -> {
            Statement stmt = conn.createStatement();

            Thread.sleep(3000);
            LOGGER.info("REFRESH testc_inventory_customers!");
            stmt.execute("REFRESH TABLE testc_inventory_customers;");

            LOGGER.info("SELECT * FROM testc_inventory_customers;");
            ResultSet itemsSet = stmt.executeQuery("SELECT id, doc FROM testc_inventory_customers ORDER BY id ASC;");

            List<Object> results = new ArrayList<>();
            while (itemsSet.next()) {
                String id = itemsSet.getString(1);
                String docJson = itemsSet.getString(2);
                Map doc = new ObjectMapper().readValue(docJson, Map.class);
                results.add(Map.of("id", id, "doc", doc));
            }
            itemsSet.close();

            List<Map<String, Object>> expectedResults = List.of(
                    Map.of("id", "\"1001\"", "doc", Map.of("last_name", "Thomas", "id", 1001, "first_name", "Sally", "email", "sally.thomas@acme.com")),
                    Map.of("id", "\"1002\"", "doc", Map.of("last_name", "Bailey", "id", 1002, "first_name", "George", "email", "gbailey@foobar.com")),
                    Map.of("id", "\"1003\"", "doc", Map.of("last_name", "Walker", "id", 1003, "first_name", "Edward", "email", "ed@walker.com")),
                    Map.of("id", "\"1004\"", "doc", Map.of("last_name", "Kretchmar", "id", 1004, "first_name", "Anne", "email", "annek@noanswer.org"))
            );

            assertThat(results).usingRecursiveComparison().isEqualTo(expectedResults);

            return true;
        });
    }

    @Test
    void testInsertsAndDeletes() {
        Testing.Print.enable();

        Awaitility.await().atMost(Duration.ofSeconds(CrateDBTestConfigSource.waitForSeconds())).until(() -> {
            final PostgresConnection connection = PostgresTestResourceLifecycleManager.getPostgresConnection();
            // data manipulation
            connection.execute("CREATE TABLE inventory.cratedb_test (id INT PRIMARY KEY, descr TEXT)");
            connection.execute("INSERT INTO inventory.cratedb_test VALUES (1, 'hello 1')");
            connection.execute("INSERT INTO inventory.cratedb_test VALUES (2, 'hello 2')");
            connection.execute("INSERT INTO inventory.cratedb_test VALUES (3, 'hello 3')");
            connection.execute("DELETE FROM inventory.cratedb_test WHERE id=1");
            connection.execute("UPDATE inventory.cratedb_test SET descr = 'hello 33' WHERE id=3");
            // schema changes
            connection.execute("ALTER TABLE inventory.cratedb_test ADD COLUMN new_col TEXT DEFAULT 'new description '");
            connection.execute("INSERT INTO inventory.cratedb_test (id, descr, new_col) VALUES (4, 'hello 4', 'new data')");
            connection.execute("INSERT INTO inventory.cratedb_test (id, descr) VALUES (5, 'hello 5')");
            connection.execute("ALTER TABLE inventory.cratedb_test DROP COLUMN new_col");
            connection.execute("ALTER TABLE inventory.cratedb_test RENAME COLUMN descr TO description");
            connection.execute("INSERT INTO inventory.cratedb_test (id, description) VALUES (6, '666')");
            connection.execute("ALTER TABLE inventory.cratedb_test ALTER COLUMN description TYPE INT USING CASE WHEN description ~ '^[0-9]+$' THEN description::integer ELSE NULL END");
            connection.execute("INSERT INTO inventory.cratedb_test (id, description) VALUES (7, 7)");

            LOGGER.info("SHOW TABLES in Postgres:");
            Thread.sleep(3000);
            connection.query("SELECT\n" +
                             "    table_schema || '.' || table_name\n" +
                             "FROM\n" +
                             "    information_schema.tables\n" +
                             "WHERE\n" +
                             "    table_type = 'BASE TABLE'\n" +
                             "AND\n" +
                             "    table_schema NOT IN ('pg_catalog', 'information_schema');", new JdbcConnection.ResultSetConsumer() {
                @Override
                public void accept(ResultSet rs) throws SQLException {
                    while (rs.next()) {
                        LOGGER.info("Table {}", rs.getString(1));
                    }
                }
            });

            LOGGER.info("PostgresSQL table state:");
            connection.query("SELECT * FROM inventory.cratedb_test", new JdbcConnection.ResultSetConsumer() {
                @Override
                public void accept(ResultSet rs) throws SQLException {
                    while (rs.next()) {
                        LOGGER.info("Row: {} {}", rs.getObject(1), rs.getString(2));
                    }
                }
            });

            connection.close();

            Statement stmt = conn.createStatement();

            Thread.sleep(3000);
            ResultSet tablesSet = stmt.executeQuery("SHOW TABLES");
            LOGGER.info("SHOW TABLES in CrateDB:");
            while (tablesSet.next()) {
                LOGGER.info("{}", tablesSet.getString(1));
            }
            tablesSet.close();

            LOGGER.info("REFRESH!");
            stmt.execute("REFRESH TABLE testc_inventory_cratedb_test;");

            LOGGER.info("SELECT * FROM testc_inventory_cratedb_test;");
            ResultSet itemsSet = stmt.executeQuery("SELECT id, doc FROM testc_inventory_cratedb_test ORDER BY id ASC;");

            List<Object> results = new ArrayList<>();
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
            List<Map<String, Object>> expectedResults = List.of(
                    Map.of("id", "\"2\"", "doc", Map.of("descr", "hello 2", "id", 2)),
                    Map.of("id", "\"3\"", "doc", Map.of("descr", "hello 33", "id", 3)),
                    Map.of("id", "\"4\"", "doc", Map.of("descr", "hello 4", "new_col", "new data", "id", 4)),
                    Map.of("id", "\"5\"", "doc", Map.of("descr", "hello 5", "new_col", "new description ", "id", 5)),
                    Map.of("id", "\"7\"", "doc", Map.of("description", 7, "id", 7))
            );

            assertThat(results).usingRecursiveComparison().isEqualTo(expectedResults);

            return true;
        });
    }
}
