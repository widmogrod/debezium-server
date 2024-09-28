/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class PostgresTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {
    public static final String POSTGRES_USER = "postgres";
    public static final String POSTGRES_PASSWORD = "postgres";
    public static final String POSTGRES_DBNAME = "postgres";
    public static final String POSTGRES_IMAGE = "quay.io/debezium/example-postgres:2.7.0.Final";
    public static final Integer POSTGRES_PORT = 5432;

    private static final GenericContainer<?> container;

    static {
        container = (new GenericContainer(POSTGRES_IMAGE))
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
                .withEnv("POSTGRES_USER", "postgres")
                .withEnv("POSTGRES_PASSWORD", "postgres")
                .withEnv("POSTGRES_DB", "postgres")
                .withEnv("POSTGRES_INITDB_ARGS", "-E UTF8")
                .withEnv("LANG", "en_US.utf8")
                .withExposedPorts(new Integer[]{ POSTGRES_PORT })
                .withStartupTimeout(Duration.ofSeconds(30L));
    }

    public PostgresTestResourceLifecycleManager() {
    }

    public static GenericContainer<?> getContainer() {
        return container;
    }

    public static PostgresConnection getPostgresConnection() {
        return new PostgresConnection(JdbcConfiguration.create()
                .with("user", POSTGRES_USER)
                .with("password", POSTGRES_PASSWORD)
                .with("dbname", POSTGRES_DBNAME)
                .with("hostname", container.getHost())
                .with("port", container.getMappedPort(POSTGRES_PORT).toString())
                .build(), "Debezium Redis Test");
    }

    public Map<String, String> start() {
        container.start();
        Map<String, String> params = new ConcurrentHashMap();
        params.put("debezium.source.database.user", POSTGRES_USER);
        params.put("debezium.source.database.password", POSTGRES_PASSWORD);
        params.put("debezium.source.database.dbname", POSTGRES_DBNAME);
        params.put("debezium.source.database.hostname", container.getHost());
        params.put("debezium.source.database.port", container.getMappedPort(POSTGRES_PORT).toString());
        return params;
    }

    public void stop() {
        try {
            if (container != null) {
                container.stop();
            }
        }
        catch (Exception var2) {
        }
    }
}
