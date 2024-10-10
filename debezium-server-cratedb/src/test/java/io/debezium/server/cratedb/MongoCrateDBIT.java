/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import static io.debezium.server.cratedb.Profile.DEBEZIUM_SOURCE_MONGODB_CONNECTION_STRING;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;

import org.awaitility.Awaitility;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import io.debezium.server.DebeziumServer;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration test that verifies basic reading from MongoDB database and writing to CrateDB stream.
 *
 * @author Gabriel Habryn
 */
@QuarkusTest
@EnabledIfEnvironmentVariable(named = DEBEZIUM_SOURCE_MONGODB_CONNECTION_STRING, matches = "mongo.*")
@TestProfile(Profile.MongoDBAndCrateDB.class)
public class MongoCrateDBIT {
    private static Connection conn;
    private static MongoClient mongo;

    private static final String connectionString = System.getenv(
            DEBEZIUM_SOURCE_MONGODB_CONNECTION_STRING);

    @Inject
    DebeziumServer server;

    @BeforeEach
    void setup() throws Exception {
        // Initialize the connection
        conn = DriverManager.getConnection(CrateTestResourceLifecycleManager.getUrl());
        mongo = MongoClients.create(connectionString);
    }

    @AfterEach
    void tearDown() throws Exception {
        // Close the connection if it's not null and not closed
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    @Test
    void testConfigIsCorrect() {
        // This is mostly sanity check, if by any chance core or upstream component
        // didn't change configuration.
        // Also, this section can help to document what are configuration settings necessary for connector to work.
        var props = server.getProps();
        assertThat(props)
                .containsEntry("connector.class", "io.debezium.connector.mongodb.MongoDbConnector")
                .containsEntry("name", "cratedb")
                .containsEntry("topic.prefix", "my_prefix")
                .containsEntry("collection.include.list", "testdb.testcollection")
                .containsEntry("transforms", "unwrap")
                .containsEntry("transforms.unwrap.type", "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState")
                .containsEntry("transforms.unwrap.add.headers", "op")
                .containsEntry("transforms.unwrap.drop.tombstones", "false")
                .containsEntry("capture.mode", "change_streams_update_full")
                .containsEntry("capture.mode.full.update.type", "lookup")
                .containsEntry("offset.storage.cratedb.connection_url", CrateTestResourceLifecycleManager.getUrl());

        assertThat(props)
                .containsEntry("mongodb.connection.string", connectionString);
    }

    @Test
    void testMongoDBConnection() throws SQLException, JsonProcessingException {
        var databases = new ArrayList<>();
        mongo.listDatabaseNames().into(databases);
        assertThat(databases).containsAll(List.of("admin", "local"));

        var db = mongo.getDatabase("testdb");
        var coll = db.getCollection("testcollection");
        coll.drop();
        coll = db.getCollection("testcollection");

        var docs = new ArrayList<Document>();
        var object1 = Map.of(
                ".", -123.31239,
                "name", 4,
                "name_integer", "Asdf");
        var document1 = new Document();
        document1.append("_id", new ObjectId("6707c4703ceb57275d16037e"))
                .append("array", Arrays.asList(53, 76, 55))
                .append("boolean", false)
                .append("date", java.util.Date.from(java.time.LocalDate.of(2024, java.time.Month.OCTOBER, 10).atStartOfDay(java.time.ZoneId.systemDefault()).toInstant()))
                .append("decimal", new Decimal128(new BigDecimal("18.261063697793077")))
                .append("double", 24.59059419857732)
                .append("int", 72)
                .append("long", 5997345523540356169L)
                .append("null", null)
                .append("obj", new Document(object1))
                .append("string", "b27692de-66c7-4516-8bfc-28da16c1e532");
        docs.add(document1);

        var object2 = Map.of(
                "$", 513.23,
                "name", "Queen");
        var document2 = new Document();
        document2.append("_id", new ObjectId("6707c4703ceb57275d16037f"))
                .append("array", Arrays.asList(37, 65, 90))
                .append("boolean", true)
                .append("date", java.util.Date.from(java.time.LocalDate.of(2024, java.time.Month.OCTOBER, 10).atStartOfDay(java.time.ZoneId.systemDefault()).toInstant()))
                .append("decimal", new Decimal128(new BigDecimal("86.9112949809932")))
                .append("double", 12.031154844035463)
                .append("int", 50)
                .append("long", -8613795666966012363L)
                .append("null", "hello".getBytes())
                .append("obj", new Document(object2))
                .append("string", "2cbde6de-8b72-4763-9364-15cc6fe5ca05");
        docs.add(document2);

        var insertResult = coll.insertMany(docs);
        assertThat(insertResult.wasAcknowledged()).isTrue();

        // Let's give some time to propagate the changes
        // First let's refresh the table that should be created by consumer
        // IF this step fails, most likely, consumer haven't process any events
        Awaitility.await().atMost(Duration.ofSeconds(Profile.waitForSeconds())).until(() -> {
            try (var stmt = conn.createStatement()) {
                try {
                    stmt.execute("REFRESH TABLE my_prefix_testdb_testcollection;");
                    var result = stmt.executeQuery("SELECT COUNT(1) FROM my_prefix_testdb_testcollection");
                    result.next();
                    return result.getInt(1) == docs.size();
                }
                catch (SQLException e) {
                    return false;
                }
            }
        });

        // Collect data from CrateDB table
        List<Object> results = new ArrayList<>();
        try (var stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("SELECT id, doc FROM my_prefix_testdb_testcollection ORDER BY id ASC;");
            while (resultSet.next()) {
                String id = resultSet.getString(1);
                String docJson = resultSet.getString(2);
                Map doc = new ObjectMapper().readValue(docJson, Map.class);
                results.add(Map.of("id", id, "doc", doc));
            }
            resultSet.close();
        }

        List<HashMap<String, Object>> expected = new ArrayList<>();
        HashMap<String, Object> firstMap = new HashMap<>() {
            {
                put("id", "6707c4703ceb57275d16037e");
                put("doc", new HashMap<String, Object>() {
                    {
                        put("_id", "6707c4703ceb57275d16037e");
                        put("array", Arrays.asList(53, 76, 55));
                        put("boolean", false);
                        put("date", 1728511200000L);
                        put("decimal", "18.261063697793077");
                        put("double", 24.59059419857732);
                        put("int", 72);
                        // NOTE non "null": null, this is because column/sub-column with null type dont' give value to strict CrateDB schema
                        // and when new column with "null" field will have a value like int, or string.
                        // It's better to crate column with schema at time when we know specific type.
                        // Second document, has "null" field set to binary.
                        put("long", 5997345523540356169L);
                        put("obj", Map.of("_dot_", -123.31239, "name", 4, "name_integer", "Asdf"));
                        put("string", "b27692de-66c7-4516-8bfc-28da16c1e532");
                    }
                });
            }
        };
        expected.add(firstMap);
        HashMap<String, Object> secondMap = new HashMap<>() {
            {
                put("id", "6707c4703ceb57275d16037f");
                put("doc", new HashMap<String, Object>() {
                    {
                        put("_id", "6707c4703ceb57275d16037f");
                        put("array", Arrays.asList(37, 65, 90));
                        put("boolean", true);
                        put("date", 1728511200000L);
                        put("decimal", "86.9112949809932");
                        put("double", 12.031154844035463);
                        put("int", 50);
                        put("null", "aGVsbG8=");
                        put("long", -8613795666966012363L);
                        put("obj", new HashMap() {
                            {
                                put("$", 513.23);
                                // INFO: this field is null, because name in first record is int, and in second string
                                // this result in type collision, and default behaviour instead of failing whole record
                                // make all the effort to save values
                                // put("name", null);
                            }
                        });
                        put("string", "2cbde6de-8b72-4763-9364-15cc6fe5ca05");
                    }
                });
            }
        };
        expected.add(secondMap);
        assertThat(results).isEqualTo(expected);
    }
}
