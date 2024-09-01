/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

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
        assertThat(CrateDBType.from("{\"name1\":\"Jon\"}").getColumnType()).isEqualTo("OBJECT AS (name1 AS TEXT)");
    }

    @Test
    void testComplexObjectTypeConversion2() {
        assertThat(CrateDBType.from("{\"name2\":[\"Jon\"]}").getColumnType()).isEqualTo("OBJECT AS (name2 AS ARRAY(TEXT))");
    }

    @Test
    void testNestedArrayObjectTypeConversion() {
        assertThat(CrateDBType.from("{\"name3\":[[\"Jon\"]]}").getColumnType()).isEqualTo("OBJECT AS (name3 AS ARRAY(ARRAY(TEXT)))");
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
}
