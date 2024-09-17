/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class CrateSQLTest {
    @Test
    void testNestedArray() {
        var beforeSchema = Schema.Dict.of();
        var afterSchema = Schema.Dict.of(
                "id", Schema.Primitive.TEXT,
                "doc", Schema.Dict.of(
                        "some-list", Schema.Array.of(Schema.Array.of(Schema.Primitive.BOOLEAN))
                )
        );

        var statements = CrateSQL.toSQL("test_table", beforeSchema, afterSchema);
        assertThat(statements).isEqualTo(List.of(
                "ALTER TABLE test_table ADD COLUMN doc['some-list'] ARRAY(ARRAY(BOOLEAN))"
        ));

        var statements2 = CrateSQL.toSQL("test_table", afterSchema, afterSchema);
        assertThat(statements2).isEqualTo(List.of());
    }
}