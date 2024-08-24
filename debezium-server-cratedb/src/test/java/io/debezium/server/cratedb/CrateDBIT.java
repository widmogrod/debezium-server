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
import java.sql.Statement;
import java.time.Duration;

import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
// @QuarkusTestResource(MySqlTestResourceLifecycleManager.class)
@QuarkusTestResource(CrateTestResourceLifecycleManager.class)
public class CrateDBIT {

    // private static final int MESSAGE_COUNT = 4;
    // // The stream of this name must exist and be empty
    // private static final String STREAM_NAME = "testc.inventory.customers";

    protected static Connection conn = null;

    {
        Testing.Files.delete(CrateDBTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(CrateDBTestConfigSource.OFFSET_STORE_PATH);
    }

    @Inject
    DebeziumServer server;

    @BeforeEach
    void setup() throws Exception {
        // Initialize the connection
        conn = DriverManager.getConnection(CrateTestResourceLifecycleManager.getUrl());
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.delete(CrateDBTestConfigSource.OFFSET_STORE_PATH);
        // Testing.Files.createTestingFile(CrateDBTestConfigSource.OFFSET_STORE_PATH);
    }

    @AfterEach
    void tearDown() throws Exception {
        // Close the connection if it's not null and not closed
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) throws Exception {
        // if (!TestConfigSource.isItTest()) {
        // return;
        // }
        conn = DriverManager.getConnection(CrateTestResourceLifecycleManager.getUrl());
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    // @Test
    // public void testPostgresWithJson() throws Exception {
    // Testing.Print.enable();
    // final TestConsumer testConsumer = (TestConsumer) server.getConsumer();
    // Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds()))
    // .until(() -> (testConsumer.getValues().size() >= MESSAGE_COUNT));
    // assertThat(testConsumer.getValues().size()).isEqualTo(MESSAGE_COUNT);
    // assertThat(((String) testConsumer.getValues().get(MESSAGE_COUNT - 1))).contains(
    // "\"after\":{\"id\":1004,\"first_name\":\"Anne\",\"last_name\":\"Kretchmar\",\"email\":\"annek@noanswer.org\"}");
    // }

    @Test
    public void testCrateDB() {
        assertThat(2).isEqualTo(2);

        Testing.Print.enable();

        Awaitility.await().atMost(Duration.ofSeconds(CrateDBTestConfigSource.waitForSeconds())).until(() -> {
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery("SELECT 2 as v");
            resultSet.next();
            return 2 == resultSet.getInt("v");
        });
    }
}
