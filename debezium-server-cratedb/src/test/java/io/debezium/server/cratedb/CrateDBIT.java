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

import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.server.DebeziumServer;
import io.debezium.server.TestConfigSource;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to Kinesis stream.
 *
 * @author Jiri Pechanec
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(CrateTestResourceLifecycleManager.class)
public class CrateDBIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrateDBIT.class);

    private static Connection conn = null;

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
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.delete(CrateDBTestConfigSource.OFFSET_STORE_PATH);
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
    public void testCrateDB() {
        Testing.Print.enable();

        Awaitility.await().atMost(Duration.ofSeconds(CrateDBTestConfigSource.waitForSeconds())).until(() -> {
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery("SELECT 2 as v");
            resultSet.next();
            assertThat(resultSet.getInt("v")).isEqualTo(2);
            resultSet.close();

            Thread.sleep(3000);
            ResultSet tablesSet = stmt.executeQuery("SHOW TABLES");
            LOGGER.info("SHOW TABLES CRATE!");
            while (tablesSet.next()) {
                LOGGER.info("{}", tablesSet.getString(1));
            }
            tablesSet.close();

            Thread.sleep(3000);
            LOGGER.info("REFRESH!");
            stmt.execute("REFRESH TABLE testc_inventory_customers;");

            Thread.sleep(3000);
            LOGGER.info("SELECT * FROM testc_inventory_customers;");
            ResultSet itemsSet = stmt.executeQuery("SELECT id, doc FROM testc_inventory_customers ORDER BY id ASC;");

            for (int i = 0; itemsSet.next(); i++) {
                String id = itemsSet.getString(1);
                String doc = itemsSet.getString(2);

                LOGGER.info("id = {}", id);
                LOGGER.info("doc = {}", doc);

                switch (i) {
                    case 0:
                        assertThat(id).isEqualTo("\"1001\"");
                        assertThat(doc).isEqualTo("{\"last_name\":\"Thomas\",\"id\":1001,\"first_name\":\"Sally\",\"email\":\"sally.thomas@acme.com\"}");
                        break;

                    case 1:
                        assertThat(id).isEqualTo("\"1002\"");
                        assertThat(doc).isEqualTo("{\"last_name\":\"Bailey\",\"id\":1002,\"first_name\":\"George\",\"email\":\"gbailey@foobar.com\"}");
                        break;

                    case 2:
                        assertThat(id).isEqualTo("\"1003\"");
                        assertThat(doc).isEqualTo("{\"last_name\":\"Walker\",\"id\":1003,\"first_name\":\"Edward\",\"email\":\"ed@walker.com\"}");
                        break;

                    case 3:
                        assertThat(id).isEqualTo("\"1004\"");
                        assertThat(doc).isEqualTo("{\"last_name\":\"Kretchmar\",\"id\":1004,\"first_name\":\"Anne\",\"email\":\"annek@noanswer.org\"}");
                        break;
                }
            }
            itemsSet.close();

            return true;
        });
    }

    @Test
    public void testCrateDB2() {
        Testing.Print.enable();

        Awaitility.await().atMost(Duration.ofSeconds(CrateDBTestConfigSource.waitForSeconds())).until(() -> {
            final PostgresConnection connection = PostgresTestResourceLifecycleManager.getPostgresConnection();
            connection.execute("CREATE TABLE inventory.cratedb_test (id INT PRIMARY KEY, descr TEXT)");
            connection.execute("INSERT INTO inventory.cratedb_test VALUES (1, 'hello 1')");
            connection.execute("INSERT INTO inventory.cratedb_test VALUES (2, 'hello 2')");
            connection.execute("INSERT INTO inventory.cratedb_test VALUES (3, 'hello 3')");
            connection.execute("DELETE FROM inventory.cratedb_test WHERE id=1");
            connection.execute("UPDATE inventory.cratedb_test SET descr = 'hello 33' WHERE id=3");

            LOGGER.info("SHOW PSQL");
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

            connection.close();

            Statement stmt = conn.createStatement();

            Thread.sleep(3000);
            ResultSet tablesSet = stmt.executeQuery("SHOW TABLES");
            LOGGER.info("SHOW TABLES CRATE!");
            while (tablesSet.next()) {
                LOGGER.info("{}", tablesSet.getString(1));
            }
            tablesSet.close();

            Thread.sleep(3000);
            LOGGER.info("REFRESH!");
            stmt.execute("REFRESH TABLE testc_inventory_cratedb_test;");

            Thread.sleep(3000);
            LOGGER.info("SELECT * FROM testc_inventory_cratedb_test;");
            ResultSet itemsSet = stmt.executeQuery("SELECT id, doc FROM testc_inventory_cratedb_test ORDER BY id ASC;");

            for (int i = 0; itemsSet.next(); i++) {
                String id = itemsSet.getString(1);
                String doc = itemsSet.getString(2);

                LOGGER.info("id = {}", id);
                LOGGER.info("doc = {}", doc);

                switch (i) {
                    case 0:
                        assertThat(id).isEqualTo("\"2\"");
                        assertThat(doc).isEqualTo("{\"descr\":\"hello 2\",\"id\":2}");
                        break;

                    case 1:
                        assertThat(id).isEqualTo("\"3\"");
                        assertThat(doc).isEqualTo("{\"descr\":\"hello 33\",\"id\":3}");
                        break;

                }
            }
            itemsSet.close();

            return true;
        });
    }
}
