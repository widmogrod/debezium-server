/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class PostgresTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {
    public static final String POSTGRES_USER = "postgres";
    public static final String POSTGRES_PASSWORD = "postgres";
    public static final String POSTGRES_DBNAME = "postgres";
    public static final String POSTGRES_IMAGE = "quay.io/debezium/example-postgres";
    public static final String POSTGRES_HOST = "localhost";
    public static final Integer POSTGRES_PORT = 5432;
    public static final String JDBC_POSTGRESQL_URL_FORMAT = "jdbc:postgresql://%s:%s/";
    private static final GenericContainer<?> container;

    public PostgresTestResourceLifecycleManager() {
    }

    public static GenericContainer<?> getContainer() {
        return container;
    }

    public Map<String, String> start() {
        container.start();
        Map<String, String> params = new ConcurrentHashMap();
        params.put("debezium.source.database.hostname", container.getHost());
        // params.put("debezium.source.database.hostname", "localhost");
        params.put("debezium.source.database.port", container.getMappedPort(POSTGRES_PORT).toString());
        params.put("debezium.source.database.user", "postgres");
        params.put("debezium.source.database.password", "postgres");
        params.put("debezium.source.database.dbname", "postgres");
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

    static {
        container = (new GenericContainer("quay.io/debezium/example-postgres"))
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
                .withEnv("POSTGRES_USER", "postgres")
                .withEnv("POSTGRES_PASSWORD", "postgres")
                .withEnv("POSTGRES_DB", "postgres")
                .withEnv("POSTGRES_INITDB_ARGS", "-E UTF8")
                .withEnv("LANG", "en_US.utf8")
                .withExposedPorts(POSTGRES_PORT)
                .withStartupTimeout(Duration.ofSeconds(30L));
    }
}
