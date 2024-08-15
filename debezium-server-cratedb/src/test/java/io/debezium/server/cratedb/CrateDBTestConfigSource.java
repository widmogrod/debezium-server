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

    public static final String KINESIS_REGION = "eu-central-1";

    public CrateDBTestConfigSource() {
        Map<String, String> cratedbTest = new HashMap<>();

        cratedbTest.put("debezium.sink.type", "cratedb");
        cratedbTest.put("debezium.sink.kinesis.region", KINESIS_REGION);
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
