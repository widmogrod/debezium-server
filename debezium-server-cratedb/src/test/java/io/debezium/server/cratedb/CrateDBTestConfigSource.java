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
        // NOTICE: debezium.source details like password, port are injected by PostgresTestResourceLifecycleManager.start()
        // remember to use @QuarkusTestResource(PostgresTestResourceLifecycleManager.class) in your integration test
        cratedbTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        cratedbTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        cratedbTest.put("debezium.source.offset.flush.interval.ms", "0");
        cratedbTest.put("debezium.source.topic.prefix", "testc");
        cratedbTest.put("debezium.source.schema.include.list", "inventory");

        // There is strange bug, and if I don't define DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST it always sets "public.table_name"
        cratedbTest.put("DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST", "inventory.customers, inventory.cratedb_test");
        // Leaving this line, if by any change, the behaviour will change
        cratedbTest.put("debezium.source.table.include.list", "inventory.customers, inventory.cratedb_test");

        cratedbTest.put("debezium.transforms", "addheader, hoist");
        cratedbTest.put("debezium.transforms.hoist.field", "payload");
        cratedbTest.put("debezium.transforms.addheader.type", "org.apache.kafka.connect.transforms.InsertHeader");
        cratedbTest.put("debezium.transforms.addheader.header", "headerKey");
        cratedbTest.put("debezium.transforms.addheader.value.literal", "headerValue");

        config = cratedbTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
