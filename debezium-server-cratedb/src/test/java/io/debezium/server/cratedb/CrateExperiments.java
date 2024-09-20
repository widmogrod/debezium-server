/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.server.cratedb.infoschema.InformationSchemaLoader;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test validating CrateDB behavior under certain conditions.
 *
 * @author Gabriel Habryn
 */
@QuarkusTest
@QuarkusTestResource(CrateTestResourceLifecycleManager.class)
public class CrateExperiments {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrateExperiments.class);

    private static Connection conn = null;
    private static final List<List<String>> results = new ArrayList<>() {
        {
            add(List.of("input", "output"));
        }
    };

    @BeforeEach
    void setup() throws Exception {
        // Initialize the connection
        conn = DriverManager.getConnection(CrateTestResourceLifecycleManager.getUrl());
    }

    @AfterEach
    void tearDown() throws Exception {
        // Close the connection if it's not null and not closed
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    @AfterAll
    public static void saveResults() {
        // Write results to CSV file
        try (java.io.FileWriter csvWriter = new java.io.FileWriter("results.csv")) {
            for (List<String> rowData : results) {
                csvWriter.append(formatCsvRow(rowData));
                csvWriter.append("\n");
            }
        }
        catch (Exception e) {
            LOGGER.error("Error writing to CSV file: {}", e.getMessage());
        }
    }

    public static class UseCase {
        public String name;
        public Object insertJSON;

        public String toString() {
            ObjectMapper mapper = new ObjectMapper();
            String json = null;
            try {
                json = mapper.writeValueAsString(insertJSON);
            }
            catch (Exception ignored) {
            }

            return "UseCase{" +
                    "name='" + name + '\'' +
                    ", insertJSON=" + json +
                    '}';
        }
    }

    @Test
    public void testJsonSerialisation() {
        List<Object> covertToTypes = generateInputs(List.of(
                "John Doe",
                123,
                98.45,
                true,
                false));

        ObjectMapper mapper = new ObjectMapper();

        for (Object type : covertToTypes) {
            LOGGER.error(" raw={}", type);
            assertDoesNotThrow(() -> {
                LOGGER.error("json={}", mapper.writeValueAsString(type));
            });
        }
    }

    @Test
    public void testOneLevelArray() {
        assertDoesNotThrow(() -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS test");
                stmt.execute("CREATE TABLE test (id TEXT PRIMARY KEY, doc OBJECT)");

                PreparedStatement prep = conn.prepareStatement("INSERT INTO test (id, doc) VALUES ('1', ?::JSON) ON CONFLICT (id) DO UPDATE SET doc = excluded.doc");
                prep.setString(1, "{\"name\": [\"John Doe\"]}");
                prep.execute();

                stmt.execute("REFRESH TABLE test");
                ResultSet result = stmt.executeQuery("SELECT doc FROM test WHERE id = '1'");
                result.next();

                assertThat(result.getString(1)).isEqualTo("{\"name\":[\"John Doe\"]}");
                result.close();

                // get type of the column doc.name and check it is a string
                ResultSet rs = stmt.executeQuery("SELECT data_type FROM information_schema.columns WHERE table_name = 'test' AND column_name = 'doc[''name'']';");
                rs.next();

                String dataType = rs.getString(1);
                assertThat(dataType).isEqualTo("text_array");
            }
        });
    }

    @Test
    public void testNestedLevelArray() {
        assertDoesNotThrow(() -> {
            String subject = "{\"name\":[[\"John Doe\"]]}";
            try (Statement stmt = conn.createStatement()) {
                // Setup the table
                stmt.execute("DROP TABLE IF EXISTS test");
                stmt.execute("CREATE TABLE test (id TEXT PRIMARY KEY, doc OBJECT)");

                // Prove that nested arrays are not supported
                Throwable throwable = assertThrows(PSQLException.class, () -> {
                    PreparedStatement prep = conn.prepareStatement("INSERT INTO test (id, doc) VALUES ('1', ?::JSON) ON CONFLICT (id) DO UPDATE SET doc = excluded.doc");
                    prep.setString(1, subject);
                    prep.execute();
                });
                assertThat(throwable.getMessage()).contains("ERROR: Dynamic nested arrays are not supported");

                // Prove that alter table does help resolve the issue
                assertDoesNotThrow(() -> {
                    stmt.execute("ALTER TABLE test ADD COLUMN doc['name'] ARRAY(ARRAY(TEXT))");
                });

                assertDoesNotThrow(() -> {
                    PreparedStatement prep = conn
                            .prepareStatement("INSERT INTO test (id, doc) VALUES ('1', ?::OBJECT) ON CONFLICT (id) DO UPDATE SET doc = excluded.doc");
                    prep.setString(1, subject);
                    prep.execute();
                });

                assertDoesNotThrow(() -> {
                    stmt.execute("REFRESH TABLE test");
                    ResultSet result = stmt.executeQuery("SELECT doc FROM test WHERE id = '1'");
                    result.next();

                    assertThat(result.getString(1)).isEqualTo(subject);
                });
            }
        });
    }

    private static @NotNull List<Object> generateInputs(List<Object> primitiveTypes) {
        List<Object> complexTypes = new ArrayList<>();
        for (Object type : primitiveTypes) {
            complexTypes.add(Map.of("key", type));
            complexTypes.add(List.of(type));
            complexTypes.add(new Object() {
                public Object key = type;
            });
        }

        List<Object> nestedTypes = new ArrayList<>();
        for (Object type : complexTypes) {
            nestedTypes.add(Map.of("key", type));
            nestedTypes.add(List.of(type));
            nestedTypes.add(new Object() {
                public Object key = type;
            });
        }

        return Stream.of(primitiveTypes, complexTypes, nestedTypes).flatMap(List::stream).toList();
    }

    private final List<CrateType> crateTypes = List.of(
            new CrateType("t_text", "TEXT"),
            new CrateType("t_bool", "BOOLEAN"),
            new CrateType("t_char", "CHARACTER(2)"),
            new CrateType("t_varchar", "VARCHAR"),
            new CrateType("t_smallint", "SMALLINT"),
            new CrateType("t_int", "INTEGER"),
            new CrateType("t_bigint", "BIGINT"),
            new CrateType("t_real", "REAL"),
            new CrateType("t_double", "DOUBLE PRECISION"),
            new CrateType("t_timestamp", "TIMESTAMP"),
            new CrateType("t_timestamp_tz", "TIMESTAMP WITH TIME ZONE"),
            new CrateType("t_timestamp_wtz", "TIMESTAMP WITHOUT TIME ZONE"),
            new CrateType("t_bit", "BIT(4)"),
            new CrateType("t_ip", "IP"),
            new CrateType("t_obj_i", "OBJECT(IGNORED)"),
            new CrateType("t_obj_d", "OBJECT(DYNAMIC)"),
            new CrateType("t_array_real", "ARRAY(REAL)"),
            new CrateType("t_float_vector", "FLOAT_VECTOR(2)"),
            new CrateType("t_geo_point", "GEO_POINT"),
            new CrateType("t_geo_shape", "GEO_SHAPE"));

    public static class CrateType {
        public String columnName;
        public String columnType;

        public CrateType(String columnName, String columnType) {
            this.columnName = columnName;
            this.columnType = columnType;
        }
    }

    private final List<TypeConflict> typeConflicts = List.of(
            new TypeConflict("Adam", "TEXT"),
            new TypeConflict(123, "BIGINT"),
            new TypeConflict(98.45, "DOUBLE PRECISION"),
            new TypeConflict(true, "BOOLEAN"),
            new TypeConflict(false, "BOOLEAN"),
            new TypeConflict("{\"name1\":\"Jon\"}", "JSON"),
            new TypeConflict("{\"name2\":\"Jon\"}", "OBJECT"),
            new TypeConflict("{\"name3\":[\"Carl\"]}", "JSON"),
            new TypeConflict("{\"name4\":[\"Carl\"]}", "OBJECT"),
            new TypeConflict("{\"name5\":[[\"Bet\"]]}", "JSON"),
            new TypeConflict("{\"name6\":[[\"Bet\"]]}", "OBJECT"),
            new TypeConflict("12:00:00"),
            new TypeConflict("13:00:00", "TIMETZ"),
            new TypeConflict("+3993-12-31T23:59:59.999Z"),
            new TypeConflict("+2024-12-31T23:59:59.999Z", "TIMESTAMP"),
            new TypeConflict("0110", "BIT"),
            new TypeConflict("0:0:0:0:0:ffff:c0a8:64"),
            new TypeConflict(new Float[]{ 4.24f, 7.1f }, "ARRAY(REAL)"),
            new TypeConflict(new Float[]{ 4.34f, 7.1f }),
            new TypeConflict(new Float[]{ 4.44f, 7.1f }, "FLOAT_VECTOR(2)"),
            new TypeConflict(new Float[][]{ { 4.54f, 7.1f } }),
            new TypeConflict("POINT (9.7417 47.4108)"),
            new TypeConflict("POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))"),
            new TypeConflict(LocalTime.now()),
            new TypeConflict(LocalDate.now()),
            new TypeConflict(LocalDateTime.now()),
            new TypeConflict(Timestamp.valueOf(LocalDateTime.now())),
            new TypeConflict(ZonedDateTime.now()),
            new TypeConflict(OffsetDateTime.now()),
            new TypeConflict(Instant.now())
    // Tested and don't work, removed from the list
    // new TypeConflict("[2.03, 31.1, 4.5, 5.6]"),
    // new TypeConflict("[1,2]"),
    // new TypeConflict("[3.14, 27.34]", "FLOAT_VECTOR"),
    // new TypeConflict("[3.24, 27.34]", "ARRAY(REAL)"),
    // new TypeConflict("ARRAY[3.34, 27.34]"),
    // new TypeConflict("ARRAY[3.44, 27.34]", "ARRAY(REAL)"),
    // new TypeConflict("{3.54, 27.34}", "ARRAY(TEXT)::ARRAY(REAL)"),
    // new TypeConflict("{3.64, 27.34}", "ARRAY(TEXT)::ARRAY(REAL)::FLOAT_VECTOR(2)"),
    // new TypeConflict(new ArrayList<>(List.of(4.44f, 7.1f)), "FLOAT_VECTOR(2)"),
    // new TypeConflict(new Vector<>(List.of(5.5f, 7.1f)), "FLOAT_VECTOR(2)"),
    // new TypeConflict(new Object() {
    // public Float[][] key_array_array_real = {{4.64f, 7.1f}};
    // })
    );

    public static class TypeConflict {
        public Object value;
        public Optional<String> typeCast = Optional.empty();

        public TypeConflict(Object value) {
            this.value = value;
        }

        public TypeConflict(Object value, String typeCast) {
            this.value = value;
            this.typeCast = Optional.of(typeCast);
        }

        public String toString() {
            return value + "::" + typeCast.orElseGet(() -> "");
        }

        public String getId() {
            return toString();
        }
    }

    @Test
    void testTypeConflictsAndConversions() {
        assertDoesNotThrow(() -> {
            try (Statement stmt = conn.createStatement()) {
                // Create table with all types
                stmt.execute("DROP TABLE IF EXISTS test");

                String sql = "CREATE TABLE test (id TEXT PRIMARY KEY, ";
                for (CrateType type : crateTypes) {
                    sql += type.columnName + " " + type.columnType + ", ";
                }
                sql = sql.substring(0, sql.length() - 2) + ")";
                stmt.execute(sql);
                LOGGER.error(sql);

                // Setup the table up front with sorted columns and rows
                Map<String, Map<String, Object>> table = new LinkedHashMap<>();
                // other columns
                for (CrateType type : crateTypes) {
                    for (TypeConflict typeConflict : typeConflicts) {
                        table.putIfAbsent(typeConflict.getId(), new LinkedHashMap<>());
                    }
                }

                // Insert values of all types
                for (TypeConflict typeConflict : typeConflicts) {
                    // But one by one to each column
                    // This should fail for some types
                    // And should succeed for others
                    // that will give us matrix of types that can be converted to each other
                    for (CrateType type : crateTypes) {
                        String insertSql = "INSERT INTO test (id, " + type.columnName + ")";
                        if (typeConflict.typeCast.isPresent()) {
                            insertSql += " VALUES (?, ?::" + typeConflict.typeCast.get() + ")";
                        }
                        else {
                            insertSql += " VALUES (?, ?)";
                        }

                        // Add on conflict clause
                        insertSql += " ON CONFLICT (id) DO UPDATE SET " + type.columnName + " = excluded." + type.columnName;

                        String id = typeConflict.getId();
                        LOGGER.error(insertSql);

                        try (PreparedStatement prep = conn.prepareStatement(insertSql)) {
                            prep.setString(1, id);
                            // Can't infer the SQL type to use for an instance of java.util.ImmutableCollections$List12. Use setObject() with an explicit Types value to specify the type to use.
                            prep.setObject(2, typeConflict.value);
                            prep.execute();
                        }
                        catch (Exception e) {
                            String firstLineOfMessage = e.getMessage().split("\n")[0];
                            table.get(id).putIfAbsent(type.columnName, firstLineOfMessage);
                            LOGGER.error("Failed to insert {} into {}; {}", typeConflict.value, type.columnName, firstLineOfMessage);
                        }
                    }
                }

                // Refresh the table
                stmt.execute("REFRESH TABLE test");

                // Check the results and add them to the table var
                ResultSet resultSet = stmt.executeQuery("SELECT * FROM test");

                // Populate the table with the results
                while (resultSet.next()) {
                    String id = resultSet.getString("id");
                    for (CrateType type : crateTypes) {
                        Object value = resultSet.getObject(type.columnName);
                        if (value != null) {
                            // if there is value, most likely error, don't put it into the table
                            table.get(id).putIfAbsent(type.columnName, value);
                        }
                    }
                }

                try (java.io.FileWriter csvWriter = new java.io.FileWriter("results2.csv")) {
                    List<String> rowData = new ArrayList<>();

                    // add column names
                    rowData.add("value");
                    for (CrateType type : crateTypes) {
                        rowData.add(type.columnName);
                    }
                    // Write to CSV
                    csvWriter.append(formatCsvRow(rowData));
                    csvWriter.append("\n");

                    // add values
                    for (Map.Entry<String, Map<String, Object>> entry : table.entrySet()) {
                        rowData.clear();
                        rowData.add(entry.getKey());
                        for (CrateType type : crateTypes) {
                            Object value = entry.getValue().get(type.columnName);
                            rowData.add(value == null ? "" : value.toString());
                        }
                        // Write to CSV
                        csvWriter.append(formatCsvRow(rowData));
                        csvWriter.append("\n");
                    }
                }
                catch (Exception e) {
                    LOGGER.error("Error writing to CSV file: {}", e.getMessage());
                }

                var info = InformationSchemaLoader.withTableName("test").load(conn);

                for (var elem : info) {
                    LOGGER.error("Column details: {}", elem);
                }

                ColumnTypeManager manager = new ColumnTypeManager();
                manager.fromInformationSchema(info);
                manager.print();
            }
        });

        assertDoesNotThrow(() -> {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT column_details, column_name, data_type, table_schema\n" +
                        "FROM information_schema.columns WHERE table_catalog = 'crate' AND table_name = 'test'";

                ResultSet rs = stmt.executeQuery(sql);

                while (rs.next()) {
                    LOGGER.error("Column details: {} {} {} {}", rs.getString("column_details"), rs.getString("column_name"), rs.getString("data_type"),
                            rs.getString("table_schema"));
                }

            }
        });
    }

    private static String formatCsvRow(List<String> rowData) {
        // Quote and escape values if necessary
        List<String> quotedData = new ArrayList<>();
        for (String value : rowData) {
            quotedData.add(quoteAndEscape(value));
        }
        return String.join(",", quotedData);
    }

    private static String quoteAndEscape(String value) {
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            value = value.replace("\"", "\"\""); // Escape double quotes
            return "\"" + value + "\""; // Quote the value
        }
        return value;
    }
}
