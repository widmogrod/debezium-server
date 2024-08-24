/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.StreamNameMapper;

/**
 * Implementation of the consumer that delivers the messages into CrateDB
 *
 * @author Gabriel Habryn
 */
@Named("cratedb")
@Dependent
public class CrateDBChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrateDBChangeConsumer.class);
    private static final String PROP_PREFIX = "debezium.sink.cratedb.";

    @ConfigProperty(name = PROP_PREFIX + "connection_url")
    String url;

    protected StreamNameMapper streamNameMapper = (x) -> x.replace(".", "_");

    // @Inject
    // Instance<KinesisClient> customClient;
    // @CustomConsumerBuilder
    private Connection conn = null;

    private HashSet<String> tablesToCreate = new HashSet<String>();

    @PostConstruct
    void connect() throws SQLException, RuntimeException {
        LOGGER.debug("Connecting to {}", url);

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
        LOGGER.debug("closing connection...");
        try {
            if (conn != null) {
                conn.close();
                LOGGER.debug("closed connection");
            }
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing cratedb client: {}", e.toString());
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        LOGGER.error("handleBatch {}", records.size());

        for (ChangeEvent<Object, Object> record : records) {
            String tableId = getTableId(record);
            LOGGER.error("Received event");

            if (!tablesToCreate.contains(tableId)) {
                tablesToCreate.add(tableId);
                LOGGER.error("Creating table '{}'", tableId);

                try {
                    String createTable = "CREATE TABLE IF NOT EXISTS %s (id varchar PRIMARY KEY, doc OBJECT(dynamic));".formatted(tableId);
                    conn.createStatement().execute(createTable);
                }
                catch (SQLException e) {
                    throw new RuntimeException("Failed to create table '%s'".formatted(tableId), e);
                }
                LOGGER.error("Table created '{}'", tableId);
            }

            // conn.createStatement().execute(
            // "INSERT INTO %s (id, doc) VALUES (%q, %q) ON CONFLICT (id) DO UPDATE SET doc = excluded.doc;"
            // .formatted(tableId, getString(record.key())));

            String upsert = "INSERT INTO %s (id, doc) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET doc = excluded.doc;".formatted(tableId);
            try {
                LOGGER.error("Prepare statement '{}'", upsert);
                PreparedStatement stmt = conn.prepareStatement(upsert);
                LOGGER.error("Prepare statement OK");

                LOGGER.error("Insert preps");
                stmt.setString(1, getString(record.key()));
                stmt.setString(2, convertToJson(record.value()));
                stmt.addBatch();

                int[] batchInserts = stmt.executeBatch();
                LOGGER.error("Batch insertion {}", batchInserts);
            }
            catch (SQLException e) {
                throw new RuntimeException("Failed in batch", e);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException("JSON serialisation failed", e);
            }

            committer.markProcessed(record);
            LOGGER.error("Processed");
        }
        committer.markBatchFinished();
        LOGGER.error("Finished");
    }

    private String convertToJson(Object object) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(object);
    }

    private String getTableId(ChangeEvent<Object, Object> record) {
        return streamNameMapper.map(record.destination());
    }
}
