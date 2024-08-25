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
import java.util.HashSet;
import java.util.List;
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
    private static final String SQL_CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s (id varchar PRIMARY KEY, doc OBJECT)";
    private static final String SQL_UPSERT = "INSERT INTO %s (id, doc) VALUES (?::varchar, ?::JSON) ON CONFLICT (id) DO UPDATE SET doc = excluded.doc";
    private static final String SQL_DELETE = "DELETE FROM %s WHERE id = ?::varchar";

    // Prefix for the configuration properties
    private static final String PROP_PREFIX = "debezium.sink.cratedb.";

    // Make sure that each table is created only once
    private final Set<String> tablesToCreate = Collections.synchronizedSet(new HashSet<>());

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

        for (ChangeEvent<Object, Object> record : records) {
            String tableId = getTableId(record);
            String recordId = getRecordId(record);
            DebeziumMessage message = getDebeziumMessage(record);
            DebeziumMessagePayload payload = message.getPayload();
            String recordDoc = getRecordDoc(payload);

            LOGGER.debug("Received event tableId: '{}', recordId: '{}', op: '{}'", tableId, recordId, payload.getOp());

            createTable(tableId);

            try {
                switch (payload.getOp()) {
                    case "r":
                    case "c":
                    case "u":
                        String upsert = SQL_UPSERT.formatted(tableId);
                        PreparedStatement stmtUpsert = conn.prepareStatement(upsert);

                        stmtUpsert.setString(1, recordId);
                        stmtUpsert.setString(2, recordDoc);
                        stmtUpsert.addBatch();
                        // TODO: make batching per type of operations and tables
                        int[] batchInserts = stmtUpsert.executeBatch();
                        break;

                    case "d":
                        String delete = SQL_DELETE.formatted(tableId);
                        LOGGER.debug("Prepare statement '{}'", delete);
                        PreparedStatement stmtDelete = conn.prepareStatement(delete);
                        stmtDelete.setString(1, recordId);
                        stmtDelete.addBatch();
                        int[] batchDeletes = stmtDelete.executeBatch();
                        LOGGER.info("Batch delete {}", batchDeletes);
                        break;

                    default:
                        LOGGER.warn("Unknown operation '{}' ignoring...", payload.getOp());
                        break;
                }
            }
            catch (SQLException e) {
                throw new RuntimeException("Failed in batch", e);
            }

            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }

    private DebeziumMessage getDebeziumMessage(ChangeEvent<Object, Object> record) {
        try {
            return serdeValue.readValue(getBytes(record.value()), DebeziumMessage.class);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getRecordId(ChangeEvent<Object, Object> record) {
        try {
            return convertToJson(serdeKey.deserializer().deserialize("xx", getBytes(record.key())));
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getRecordDoc(DebeziumMessagePayload payload) {
        try {
            return convertToJson(payload.getAfter());
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void createTable(String tableId) {
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
