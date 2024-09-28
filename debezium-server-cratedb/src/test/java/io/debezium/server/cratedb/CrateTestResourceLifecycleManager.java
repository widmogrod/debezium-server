/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.cratedb;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * CrateDB resource lifecycle https://cratedb.com/docs/jdbc/en/latest/index.html
 */
public class CrateTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrateTestResourceLifecycleManager.class);

    public static final String CRATEDB_IMAGE = "library/crate:5.8.1";
    public static final Integer CRATEDB_PORT = 5432;

    // ISSUE jdbc:crate:// does not work as documented
    // https://cratedb.com/docs/jdbc/en/latest/index.html
    public static final String JDBC_CRATE_URL_FORMAT = "jdbc:postgresql://%s:%s/?user=crate";

    private static final GenericContainer<?> container;

    public static String getUrl() {
        // Ensure the container is running and the port is mapped before accessing it
        if (!container.isRunning()) {
            throw new IllegalStateException("Container is not running");
        }

        Integer mappedPort = container.getMappedPort(CRATEDB_PORT);
        if (mappedPort == null) {
            throw new IllegalArgumentException("Requested port (" + CRATEDB_PORT + ") is not mapped");
        }

        return String.format(JDBC_CRATE_URL_FORMAT,
                container.getHost(),
                container.getMappedPort(CRATEDB_PORT).toString());
    }

    @Override
    public Map<String, String> start() {
        container.start();
        Map<String, String> params = new ConcurrentHashMap();
        params.put("debezium.sink.cratedb.connection_url", getUrl());
        params.put("debezium.source.schema.include.list", "inventory");
        params.put("debezium.source.table.include.list", "inventory.customers,inventory.cratedb_test");

        LOGGER.info("CrateTestResourceLifecycleManager started with params: {}", params);
        return params;
    }

    public void stop() {
        try {
            if (container != null) {
                container.stop();
            }
        }
        catch (Exception var2) {
            LOGGER.debug("Error stopping CrateTestResourceLifecycleManager", var2);
        }
    }

    static {
        container = (new GenericContainer(CRATEDB_IMAGE))
                .waitingFor(Wait.forLogMessage("(.*)started(.*)", 1))
                .withExposedPorts(CRATEDB_PORT,
                        4200)
                .withEnv("CRATE_HEAP_SIZE", "1g")
                .withStartupTimeout(Duration.ofSeconds(30L));
    }
}
