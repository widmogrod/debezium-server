/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class CrateDBTestConfigSource extends TestConfigSource {

    public CrateDBTestConfigSource() {
        Map<String, String> cratedbTest = new HashMap<>();

        cratedbTest.put("debezium.sink.type", "cratedb");
        // cratedbTest.put("debezium.sink.cratedb.connection_url", "jdbc:postgresql://%s:%s/?user=crate");
        // cratedbTest.put("debezium.source.connector.class", "io.debezium.connector.mysql.MySqlConnector");
        // cratedbTest.put("debezium.source.database.hostname", "mysql");
        // cratedbTest.put("debezium.source.database.port", "3306");
        // cratedbTest.put("debezium.source.database.user", "debezium");
        // cratedbTest.put("debezium.source.database.password", "dbz");
        // cratedbTest.put("debezium.source.database.server.id", "23");
        // cratedbTest.put("debezium.sink.kinesis.region", KINESIS_REGION);
        cratedbTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        cratedbTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        cratedbTest.put("debezium.source.offset.flush.interval.ms", "0");
        cratedbTest.put("debezium.source.topic.prefix", "testc");
        cratedbTest.put("debezium.source.schema.include.list", "inventory");
        cratedbTest.put("debezium.source.table.include.list", "inventory.customers");

        config = cratedbTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
