/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.cratedb.infoschema.DataLoader;
import io.debezium.server.cratedb.infoschema.SchemaBuilder;
import io.debezium.server.cratedb.schema.Evolution;
import io.debezium.server.cratedb.schema.Schema;

/**
 * Implementation of the consumer that delivers the messages into CrateDB
 *
 * @author Gabriel Habryn
 */
@Named("cratedb")
@Dependent
public class CrateDBChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    // Logger
    private static final Logger LOGGER = LoggerFactory.getLogger(CrateDBChangeConsumer.class);

    // SQL statements used to interact with the CrateDB
    private static final String SQL_CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s (id varchar PRIMARY KEY, doc OBJECT) WITH (\"mapping.total_fields.limit\" = 20000)";
    private static final String SQL_UPSERT = "INSERT INTO %s (id, doc) VALUES (?::varchar, ?::JSON) ON CONFLICT (id) DO UPDATE SET doc = excluded.doc";
    private static final String SQL_DELETE = "DELETE FROM %s WHERE id = ?::varchar";

    // Prefix for the configuration properties
    private static final String PROP_PREFIX = "debezium.sink.cratedb.";

    // Make sure that each table is created only once
    private final Set<String> tablesToCreate = Collections.synchronizedSet(new HashSet<>());
    // Make sure that track each table and schema evolution independently
    private final Map<String, Schema.I> tablesSchema = Collections.synchronizedMap(new HashMap<>());

    // Manage extraction of id and document from the Debzium message
    // https://debezium.io/documentation/reference/stable/integrations/serdes.html
    final Serde<String> serdeKey = DebeziumSerdes.payloadJson(String.class);
    final ObjectMapper serdeValue = new ObjectMapper();

    // Configuration properties
    @ConfigProperty(name = PROP_PREFIX + "connection_url")
    String url;

    // Database connection
    private Connection conn = null;

    @PostConstruct
    void connect() throws SQLException, RuntimeException {
        LOGGER.info("Connecting to {}", url);

        if (conn == null) {
            conn = DriverManager.getConnection(url);
            // conn.setAutoCommit(false);
            LOGGER.debug("New connection established");
        }

        // check if is connected
        if (conn.isClosed()) {
            LOGGER.error("Driver connection is closed");
            throw new RuntimeException("Driver connection is closed");
        }

        ResultSet pingStmt = conn.createStatement().executeQuery("SELECT 1");
        if (!pingStmt.next()) {
            LOGGER.error("Ping query returned no results");
            throw new RuntimeException("Ping query returned no results");
        }

        if (pingStmt.getInt(1) != 1) {
            LOGGER.error("Ping query returned '{}' but expected 1", pingStmt.getInt(0));
            throw new RuntimeException("Ping query returned '" + pingStmt.getString(0) + "' but expected 1");
        }

        LOGGER.info("Connected");
    }

    @PreDestroy
    void close() {
        LOGGER.info("closing connection...");
        try {
            if (conn != null) {
                conn.close();
                LOGGER.info("closed connection");
            }
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing cratedb client: {}", e.toString());
        }
    }

    @PostConstruct
    void setupDeserializer() {
        // normalise the table names for CrateDB
        streamNameMapper = (x) -> x.replace(".", "_");
        // key extraction will be used as table id
        serdeKey.configure(Collections.emptyMap(), true);
        // value will be deserialized as DebeziumMessage to properly handle insert update and delete
        serdeValue.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer) throws InterruptedException {
        LOGGER.info("handleBatch of size {}", records.size());

        // Deduplicate records by tableId and recordId leave only the latest record.
        Map<String, Map<String, ChangeEvent<Object, Object>>> recordMap = new LinkedHashMap<>();
        for (ChangeEvent<Object, Object> record : records) {
            String tableId = getTableId(record);
            try {
                String recordId = getRecordId(record);
                recordMap.computeIfAbsent(tableId, k -> new LinkedHashMap<>()).put(recordId, record);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        // Process the records per each table
        for (Map.Entry<String, Map<String, ChangeEvent<Object, Object>>> tableEntry : recordMap.entrySet()) {
            String tableId = tableEntry.getKey();
            maybeCreateTable(tableId);

            PreparedStatement stmtUpsert = null;
            PreparedStatement stmtDelete = null;

            // Retrieve schema definition
            Schema.I schema0;
            if (tablesSchema.containsKey(tableId)) {
                schema0 = tablesSchema.get(tableId);
            }
            else {
                try {
                    var columns = DataLoader.withTableName(tableId).load(conn);
                    schema0 = SchemaBuilder.fromInformationSchema(columns);
                    var subElement = Evolution.fromPath(List.of("doc"), schema0);
                    schema0 = subElement.orElseGet(Schema.Dict::of);
                }
                catch (Exception e) {
                    LOGGER.error("Error while loading table {}", tableId, e);
                    schema0 = Schema.Dict.of();
                }
            }

            var tableRows = tableEntry.getValue().entrySet();
            for (Map.Entry<String, ChangeEvent<Object, Object>> recordEntry : tableRows) {
                String recordId = recordEntry.getKey();
                ChangeEvent<Object, Object> record = recordEntry.getValue();
                String operation = getOperationType(record);

                try {
                    switch (operation) {
                        case "c", "r", "u":
                            var object0 = getRecordObject(record);
                            var result = Evolution.fromObject(schema0, object0);
                            var schema1 = result.getLeft();
                            var object1 = result.getRight();
                            // update what we learn about schema
                            schema0 = schema1;
                            tablesSchema.put(tableId, schema1);
                            // use final representation of object as something we will insert into database
                            String recordDoc = getRecordDoc(object1);

                            if (stmtUpsert == null) {
                                String upsert = SQL_UPSERT.formatted(tableId);
                                stmtUpsert = conn.prepareStatement(upsert);
                            }
                            stmtUpsert.setString(1, recordId);
                            stmtUpsert.setString(2, recordDoc);
                            stmtUpsert.addBatch();
                            break;

                        case "d":
                            if (stmtDelete == null) {
                                String delete = SQL_DELETE.formatted(tableId);
                                stmtDelete = conn.prepareStatement(delete);
                            }
                            stmtDelete.setString(1, recordId);
                            stmtDelete.addBatch();
                            break;

                        default:
                            LOGGER.warn("Unknown operation '{}' ignoring...", operation);
                            break;
                    }
                }
                catch (SQLException | IOException e) {
                    closeStatements(stmtUpsert, stmtDelete);
                    throw new RuntimeException("Failed in prepare", e);
                }
            }

            try {
                if (stmtUpsert != null) {
                    int[] processed = stmtUpsert.executeBatch();
                    for (int i = 0; i < processed.length; i++) {
                        if (processed[i] != 1) {
                            // FIXME: This is silent failure, allow configuration of this behaviour
                            LOGGER.warn("Failed to upsert record tableId={} id={} code={}", tableId, tableRows.stream().toList().get(i).getKey(), processed[i]);
                        }
                    }
                }
                if (stmtDelete != null) {
                    int[] processed = stmtDelete.executeBatch();
                    for (int i = 0; i < processed.length; i++) {
                        if (processed[i] != 1) {
                            // FIXME: This is silent failure, allow configuration of this behaviour
                            LOGGER.warn("Failed to delete record tableId={} id={} code={}", tableId, tableRows.stream().toList().get(i).getKey(), processed[i]);
                        }
                    }
                }
            }
            catch (SQLException e) {
                throw new RuntimeException("Failed in batch", e);
            }
            finally {
                closeStatements(stmtUpsert, stmtDelete);
            }
        }

        // mark everything as processed
        for (ChangeEvent<Object, Object> record : records) {
            committer.markProcessed(record);
        }

        committer.markBatchFinished();
    }

    private static void closeStatements(PreparedStatement stmtUpsert, PreparedStatement stmtDelete) {
        if (stmtUpsert != null) {
            try {
                stmtUpsert.close();
            }
            catch (SQLException e) {
                LOGGER.warn("Failed to close upsert statement", e);
            }
        }
        if (stmtDelete != null) {
            try {
                stmtDelete.close();
            }
            catch (SQLException e) {
                LOGGER.warn("Failed to close delete statement", e);
            }
        }
    }

    private Object getRecordObject(ChangeEvent<Object, Object> record) throws IOException {
        Object value = record.value();
        if (value == null) {
            return value;
        }

        var bytes = getBytes(value);
        return serdeValue.readValue(bytes, Object.class);
    }

    private String getRecordId(ChangeEvent<Object, Object> record) throws JsonProcessingException {
        var key = serdeKey.deserializer().deserialize("xx", getBytes(record.key()));
        if (key instanceof String str) {
            return str;
        }
        return convertToJson(key);
    }

    private String getOperationType(ChangeEvent<Object, Object> record) {
        String operation = "unknown";
        for (var header : record.headers()) {
            if (Objects.equals(header.getKey(), "__op")) {
                var x = serdeKey.deserializer().deserialize("xx", getBytes(header.getValue()));
                operation = x.replace("\"", "");
                break;
            }
        }
        return operation;
    }

    private String getRecordDoc(Object object) {
        try {
            return convertToJson(object);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void maybeCreateTable(String tableId) {
        if (tablesToCreate.contains(tableId)) {
            return;
        }
        tablesToCreate.add(tableId);

        String createTable = SQL_CREATE_TABLE.formatted(tableId);
        try {
            try (Statement stmtCreate = conn.createStatement()) {
                stmtCreate.execute(createTable);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String convertToJson(Object object) throws JsonProcessingException {
        return serdeValue.writeValueAsString(object);
    }

    private String getTableId(ChangeEvent<Object, Object> record) {
        return streamNameMapper.map(record.destination());
    }
}
