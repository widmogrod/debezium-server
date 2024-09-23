/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

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
                "ALTER TABLE \"test_table\" ADD COLUMN \"doc['some-list']\" ARRAY(ARRAY(BOOLEAN))"));

        var statements2 = CrateSQL.toSQL("test_table", afterSchema, afterSchema);
        assertThat(statements2).isEqualTo(List.of());
    }

    @Test
    void testColiToIgnore() {
        var beforeSchema = Schema.Dict.of();
        var afterSchema = Schema.Dict.of(
                "id", Schema.Coli.of(
                        Schema.Primitive.BOOLEAN,
                        Schema.Array.of(Schema.Array.of(Schema.Primitive.BOOLEAN))
                ),
                "doc", Schema.Dict.of(
                        "some-list", Schema.Coli.of(
                                Schema.Primitive.BOOLEAN,
                                Schema.Array.of(Schema.Array.of(Schema.Primitive.BOOLEAN))
                        ),
                        "other-list", Schema.Array.of(
                                Schema.Array.of(
                                        Schema.Array.of(
                                                Schema.Coli.of(
                                                        Schema.Primitive.BOOLEAN,
                                                        Schema.Array.of(Schema.Array.of(Schema.Primitive.BOOLEAN))
                                                )
                                        )
                                )
                        )
                )
        );

        var statements = CrateSQL.toSQL("test_table", beforeSchema, afterSchema);
        assertThat(statements).isEqualTo(List.of(
//                "ALTER TABLE \"test_table\" ADD COLUMN \"id\" OBJECT(IGNORED)"
//                "ALTER TABLE \"test_table\" ADD COLUMN \"doc['some-list']\" OBJECT(IGNORED)",
                "ALTER TABLE \"test_table\" ADD COLUMN \"doc['other-list']\" ARRAY(ARRAY(ARRAY(OBJECT(IGNORED))))"
        ));

        var statements2 = CrateSQL.toSQL("test_table", afterSchema, afterSchema);
        assertThat(statements2).isEqualTo(List.of());
    }
}
