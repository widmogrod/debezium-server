/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTestProfile;

public class Profile {

    public static int waitForSeconds() {
        return 60;
    }

    @QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
    @QuarkusTestResource(CrateTestResourceLifecycleManager.class)
    public static class PostgresAndCrateDB implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> params = new ConcurrentHashMap<>();

            params.put("debezium.sink.type", "cratedb");
            // Other params from CrateTestResourceLifecycleManager:
            // - "debezium.sink.cratedb.connection_url"

            params.put("debezium.transforms", "unwrap");
            params.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
            params.put("debezium.transforms.unwrap.add.headers", "op");
            params.put("debezium.transforms.unwrap.drop.tombstones", "false");

            // Other params like username, port, etc be taken from PostgresTestResourceLifecycleManager
            // - "debezium.source.database.user"
            // - "debezium.source.database.password"
            // - "debezium.source.database.dbname"
            // - "debezium.source.database.hostname"
            // - "debezium.source.database.port"
            params.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
            params.put("debezium.source.offset.flush.interval.ms", "0");
            params.put("debezium.source.topic.prefix", "testc");
            params.put("debezium.source.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");

            // There is strange bug, and if I don't define DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST it always sets "public.table_name"
            params.put("DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST", "inventory.customers, inventory.cratedb_test");
            params.put("debezium.source.schema.include.list", "inventory");
            params.put("debezium.source.table.include.list", "inventory.customers,inventory.cratedb_test");

            // CrateDB sink can infer schema on it's own
            // no need to send schema information
            params.put("debezium.format.schemas.enable", "false");
            params.put("debezium.format.header.schemas.enable", "false");
            params.put("debezium.format.value.schemas.enable", "false");

            // https://debezium.io/documentation/reference/stable/operations/debezium-server.html#debezium-format-configuration-options
            params.put("debezium.format.key", "json");
            params.put("debezium.format.value", "json");
            params.put("debezium.format.header", "json");
            return params;
        }
    }

}
