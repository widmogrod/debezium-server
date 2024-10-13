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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.server.cratedb.schema.CrateSQL;
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

import io.debezium.DebeziumException;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
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
    public static final String SQL_CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS %s (
                id varchar PRIMARY KEY,
                doc OBJECT,
                malformed OBJECT(IGNORED),
                err TEXT
            )
            WITH ("mapping.total_fields.limit" = 20000)
            """;
    public static final String SQL_UPSERT = """
            INSERT INTO %s (id, doc, malformed, err)
            VALUES (?::varchar, ?::JSON, ?::JSON, NULL)
            ON CONFLICT (id) DO UPDATE
                SET doc = excluded.doc
                  , malformed = excluded.malformed
                  , err = excluded.err
            """;
    public static final String SQL_UPSERT_ERR = """
            INSERT INTO %s (id, err)
            VALUES (?::varchar, ?::TEXT)
            ON CONFLICT (id) DO UPDATE
                SET err = excluded.err
            """;
    public static final String SQL_DELETE = "DELETE FROM %s WHERE id = ?::varchar";

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

    @ConfigProperty(name = PROP_PREFIX + "type_conflict_strategy", defaultValue = TypeConflictResolution.NAME_SILENT_NULL)
    String strategyName;

    private TypeConflictResolution.Strategy strategyType;

    // Database connection
    private Connection conn = null;

    @PostConstruct
    void start() {
        try {
            LOGGER.info("Connecting to {}", url);

            if (conn == null) {
                conn = DriverManager.getConnection(url);
                conn.setAutoCommit(false);
                LOGGER.debug("New connection established");
            }

            // check if is connected
            if (conn.isClosed()) {
                LOGGER.error("Driver connection is closed");
                throw new DebeziumException("Driver connection is closed");
            }

            ResultSet pingStmt = conn.createStatement().executeQuery("SELECT 1");
            if (!pingStmt.next()) {
                LOGGER.error("Ping query returned no results");
                throw new DebeziumException("Ping query returned no results");
            }

            if (pingStmt.getInt(1) != 1) {
                LOGGER.error("Ping query returned '{}' but expected 1", pingStmt.getInt(0));
                throw new DebeziumException("Ping query returned '" + pingStmt.getString(0) + "' but expected 1");
            }

            LOGGER.info("Connected");
        }
        catch (SQLException e) {
            conn = null;
            throw new DebeziumException("Failed to initialise connection to CrateDB", e);
        }
    }

    @PreDestroy
    void stop() {
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
        finally {
            conn = null;
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
                String recordId = getRecordId(record.key());
                recordMap.computeIfAbsent(tableId, k -> new LinkedHashMap<>()).put(recordId, record);
            }
            catch (JsonProcessingException e) {
                throw new DebeziumException("Failure during deduplication of batch", e);
            }
        }

        // Process the records per each table
        for (Map.Entry<String, Map<String, ChangeEvent<Object, Object>>> tableEntry : recordMap.entrySet()) {
            String tableId = tableEntry.getKey();
            maybeCreateTable(tableId);

            PreparedStatement stmtUpsert = null;
            PreparedStatement stmtDelete = null;
            // store position of inserts and updates for retry logic
            Map<Number, String> insertPositionToRecordId = new HashMap<>();
            Map<Number, String> deletePositionToRecordId = new HashMap<>();

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
                            insertPositionToRecordId.put(insertPositionToRecordId.size(), recordId);

                            var object0 = getRecordObject(record);
                            var result = Evolution.fromObject(schema0, object0);
                            var schema1 = result.getLeft();
                            var object1 = result.getRight();
                            // perform alter
                            var alters = CrateSQL.toSQL(tableId, schema0, schema1);
                            for (var alter : alters) {
                                try {
                                    try (var stmt = conn.createStatement()) {
                                        stmt.executeUpdate(alter.toString());
                                    }
                                }
                                catch (SQLException e) {
                                    LOGGER.warn("Error while executing alter {}", alter, e);
                                }
                            }

                            // update what we learn about schema
                            schema0 = schema1;
                            tablesSchema.put(tableId, schema1);

                            // use final representation of object as something we will insert into database
                            String recordDoc = getRecordDoc(schema1, object1);
                            String malformedDoc = getMalformedDoc(schema1, object1);

                            if (stmtUpsert == null) {
                                String upsert = SQL_UPSERT.formatted(tableId);
                                stmtUpsert = conn.prepareStatement(upsert);
                            }
                            stmtUpsert.setString(1, recordId);
                            stmtUpsert.setString(2, recordDoc);
                            stmtUpsert.setString(3, malformedDoc);
                            stmtUpsert.addBatch();
                            break;

                        case "d":
                            deletePositionToRecordId.put(deletePositionToRecordId.size(), recordId);
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
                    throw new DebeziumException("Failed in preparing and batching", e);
                }
            }

            try {
                if (stmtUpsert != null) {
                    int[] processed = stmtUpsert.executeBatch();
                    for (int i = 0; i < processed.length; i++) {
                        if (processed[i] != 1) {
                            String recordId = insertPositionToRecordId.get(i);
                            var element = tableRows.stream().filter((v) -> v.getKey().equals(recordId)).findFirst().get();

                            try {
                                var schema00 = tablesSchema.get(tableId);
                                var object00 = getRecordObject(element.getValue());
                                var result00 = Evolution.fromObject(schema00, object00);
                                var recordDoc = getRecordDoc(schema00, result00.getRight());
                                var malformedDoc = getMalformedDoc(schema00, result00.getRight());

                                String upsert = SQL_UPSERT.formatted(tableId);
                                try (var stmt = conn.prepareStatement(upsert)) {
                                    stmt.setString(1, recordId);
                                    stmt.setString(2, recordDoc);
                                    stmt.setString(3, malformedDoc);
                                    stmt.execute();
                                }
                            }
                            catch (SQLException | IOException e) {
                                try {
                                    String upsert = SQL_UPSERT_ERR.formatted(tableId);
                                    try (var stmt = conn.prepareStatement(upsert)) {
                                        stmt.setString(1, recordId);
                                        stmt.setString(2, getErrorDoc(e));
                                        stmt.execute();
                                    }
                                }
                                catch (SQLException e2) {
                                    LOGGER.warn("Failed to upsert record tableId={} id={} code={}, exception={}, previous exception={}",
                                            tableId, recordId, processed[i], e2.getMessage(), e.getMessage());
                                }
                            }
                        }
                    }
                }
                if (stmtDelete != null) {
                    int[] processed = stmtDelete.executeBatch();
                    for (int i = 0; i < processed.length; i++) {
                        // previously was != 1, but it looks like every delete returns 0
                        if (processed[i] != 0) {
                            String recordId = deletePositionToRecordId.get(i);
                            try {
                                String delete = SQL_DELETE.formatted(tableId);
                                try (var stmt = conn.prepareStatement(delete)) {
                                    stmt.setString(1, recordId);
                                    stmt.execute();
                                }
                            }
                            catch (SQLException e) {
                                try {
                                    String upsert = SQL_UPSERT_ERR.formatted(tableId);
                                    try (var stmt = conn.prepareStatement(upsert)) {
                                        stmt.setString(1, recordId);
                                        stmt.setString(2, getErrorDoc(e));
                                        stmt.execute();
                                    }
                                }
                                catch (SQLException e2) {
                                    LOGGER.warn("Failed to delete record tableId={} id={} code={}, exception={} previous exception={}",
                                            tableId, recordId, processed[i], e2.getMessage(), e.getMessage());
                                }
                            }
                        }
                    }
                }
            }
            catch (SQLException e) {
                throw new DebeziumException("Failed in executing batch", e);
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
            return null;
        }

        if (record instanceof EmbeddedEngineChangeEvent<?, ?, ?> che) {
            var source = che.sourceRecord();
            return KafkaDataToJavaLangConverter.convertToJavaObject(source.valueSchema(), source.value());
        }

        var bytes = getBytes(value);
        var result = serdeValue.readValue(bytes, Object.class);
        result = Evolution.dedebeziumArrayDocuments(result);
        return result;
    }

    private String getRecordId(Object recordKey) throws JsonProcessingException {
        var key = serdeKey.deserializer().deserialize("xx", getBytes(recordKey));
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

    private String getRecordDoc(Schema.I schema, Object object) {
        try {
            if (getStrategyType() == TypeConflictResolution.Strategy.TYPE_SUFFIX_AND_MALFORMED) {
                object = Evolution.typeSuffix(schema, object);
            }

            return convertToJson(object);
        }
        catch (JsonProcessingException e) {
            throw new DebeziumException("Failed serialize document to JSON", e);
        }
    }

    private String getMalformedDoc(Schema.I schema, Object object) {
        try {
            return switch (getStrategyType()) {
                case STORE_MALFORMED_FRAGMENTS -> convertToJson(Evolution.extractNonCasted(object));
                case TYPE_SUFFIX_AND_MALFORMED -> convertToJson(Evolution.extractNonCasted(Evolution.typeSuffix(schema, object)));
                default -> null;
            };
        }
        catch (JsonProcessingException e) {
            throw new DebeziumException("Failed serialize malformed document as JSON", e);
        }
    }

    private String getErrorDoc(Exception exception) {
        return Arrays.stream(exception.getMessage().split("\n")).limit(1).collect(Collectors.joining("\n"));
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
            throw new DebeziumException("Failed create table '%s'".formatted(tableId), e);
        }
    }

    private String convertToJson(Object object) throws JsonProcessingException {
        return serdeValue.writeValueAsString(object);
    }

    private String getTableId(ChangeEvent<Object, Object> record) {
        return streamNameMapper.map(record.destination());
    }

    private TypeConflictResolution.Strategy getStrategyType() {
        if (strategyType == null) {
            strategyType = TypeConflictResolution.fromString(strategyName);
            LOGGER.info("Using strategy '{}' value {}", TypeConflictResolution.stringify(strategyType), strategyName);
        }

        return strategyType;
    }
}
