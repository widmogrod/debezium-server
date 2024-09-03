/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Test how CrateDB types are mapped to CrateDB types.
 *
 * @author Gabriel Habryn
 */
public class CrateTypeMapper {
    @Test
    void testTextTypeConversion() {
        assertThat(CrateDBType.from("Jon").getColumnType()).isEqualTo("TEXT");
    }

    @Test
    void testObjectTypeConversion() {
        assertThat(CrateDBType.from("{}").getColumnType()).isEqualTo("OBJECT");
    }

    @Test
    void testBigIntTypeConversion() {
        assertThat(CrateDBType.from("1234").getColumnType()).isEqualTo("BIGINT");
    }

    @Test
    void testRealTypeConversion() {
        assertThat(CrateDBType.from("1234.123").getColumnType()).isEqualTo("REAL");
    }

    @Test
    void testComplexObjectTypeConversion1() {
        assertThat(CrateDBType.from("{\"name1\":\"Jon\"}").getColumnType()).isEqualTo("OBJECT AS (name1 TEXT)");
    }

    @Test
    void testComplexObjectTypeConversion2() {
        assertThat(CrateDBType.from("{\"name2\":[\"Jon\"]}").getColumnType()).isEqualTo("OBJECT AS (name2 ARRAY(TEXT))");
    }

    @Test
    void testNestedArrayObjectTypeConversion() {
        assertThat(CrateDBType.from("{\"name3\":[[\"Jon\"]]}").getColumnType()).isEqualTo("OBJECT AS (name3 ARRAY(ARRAY(TEXT)))");
    }

    @Test
    void testPolygonTypeConversion() {
        assertThat(CrateDBType.from("POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))").getColumnType()).isEqualTo("TEXT");
    }

    @Test
    void testPointTypeConversion() {
        assertThat(CrateDBType.from("POINT (9.7417 47.4108)").getColumnType()).isEqualTo("GEO_POINT");
    }

    @Test
    void testIPAddressTypeConversion() {
        assertThat(CrateDBType.from("0:0:0:0:0:ffff:c0a8:64").getColumnType()).isEqualTo("TEXT");
    }

    @Test
    void testTimeWithZoneTypeConversion() {
        assertThat(CrateDBType.from("13:00:00").getColumnType()).isEqualTo("TIMETZ");
    }

    @Test
    void testDateTimeWithZoneTypeConversion() {
        assertThat(CrateDBType.from("2024-12-31T23:59:59.999Z").getColumnType()).isEqualTo("TIMETZ");
    }

    @Test
    void testBigIntArrayTypeConversion() {
        assertThat(CrateDBType.from("[1,2]").getColumnType()).isEqualTo("ARRAY(BIGINT)");
    }

    @Test
    void testRealArrayTypeConversion() {
        assertThat(CrateDBType.from("[0.1, 2.2]").getColumnType()).isEqualTo("ARRAY(REAL)");
    }

    @Test
    void testWrapObject() {
        assertDoesNotThrow(() -> {
            String input = """
                    {
                      "name1": "Jon",
                      "po": [
                        {
                          "a": 1
                        },
                        {
                          "a": "Test",
                          "b": [
                            [
                              {
                                "c": 1
                              },
                              {
                                "c": [false]
                              }
                            ]
                          ]
                        },
                        1,
                        false
                      ]
                    }""";
            ObjectMapper mapper = new ObjectMapper();
            Object object = mapper.readValue(input, Object.class);
            Object transformed = CrateDBType.wrap(object);

            String output = mapper.writeValueAsString(transformed);

            assertThat(output).isNotEqualTo(input);
            assertThat(output).isEqualTo("""
                    {"name1_t":"Jon","po_arr_o":[{"a_i":1},{"a_t":"Test","b_arr_arr_o":[[{"c_i":1},{"c_arr_t":[false]}]]},1,false]}""");
        });
    }
}
