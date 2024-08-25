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
            LOGGER.debug("Received event");

            if (!tablesToCreate.contains(tableId)) {
                tablesToCreate.add(tableId);
                LOGGER.debug("Creating table '{}'", tableId);

                try {
                    String createTable = "CREATE TABLE IF NOT EXISTS %s (id varchar PRIMARY KEY, doc OBJECT);".formatted(tableId);
                    conn.createStatement().execute(createTable);
                }
                catch (SQLException e) {
                    throw new RuntimeException("Failed to create table '%s'".formatted(tableId), e);
                }
                LOGGER.debug("Table created '{}'", tableId);
            }

            LOGGER.info("Insert preps {}", record.value());

            try {
                String recordId = convertToJson(serdeKey.deserializer().deserialize("xx", getBytes(record.key())));
                DebeziumMessage message = serdeValue.readValue(getBytes(record.value()), DebeziumMessage.class);

                DebeziumMessagePayload payload = message.getPayload();

                switch (payload.getOp()) {
                    case "r":
                    case "c":
                    case "u":
                        String upsert = "INSERT INTO %s (id, doc) VALUES (?::varchar, ?::JSON) ON CONFLICT (id) DO UPDATE SET doc = excluded.doc;".formatted(tableId);
                        LOGGER.debug("Prepare statement '{}'", upsert);
                        PreparedStatement stmtUpsert = conn.prepareStatement(upsert);

                        LOGGER.info("Headers: {}", record.headers().toArray());
                        stmtUpsert.setString(1, recordId);
                        stmtUpsert.setString(2, convertToJson(payload.getAfter()));
                        stmtUpsert.addBatch();
                        int[] batchInserts = stmtUpsert.executeBatch();
                        LOGGER.info("Batch insertion {}", batchInserts);
                        break;

                    case "d":
                        String delete = "DELETE FROM %s WHERE id = ?::varchar;".formatted(tableId);
                        LOGGER.debug("Prepare statement '{}'", delete);
                        PreparedStatement stmtDelete = conn.prepareStatement(delete);
                        stmtDelete.setString(1, recordId);
                        stmtDelete.addBatch();
                        int[] batchDeletes = stmtDelete.executeBatch();
                        LOGGER.info("Batch delete {}", batchDeletes);
                        break;

                    default:
                        LOGGER.warn("Unknown operation '{}' ignoring", payload.getOp());
                        break;
                }
            }
            catch (SQLException e) {
                throw new RuntimeException("Failed in batch", e);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException("JSON serialisation failed", e);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            committer.markProcessed(record);
        }
        committer.markBatchFinished();
        LOGGER.debug("Finished batch");
    }

    private String convertToJson(Object object) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(object);
    }

    private String getTableId(ChangeEvent<Object, Object> record) {
        return streamNameMapper.map(record.destination());
    }
}
