/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class EvolutionTest {
    @Test
    void testFromPath() {
        var schema = Schema.Dict.of(
                "a", Schema.Dict.of(
                        "b", Schema.Array.of(Schema.Dict.of(
                                "c", Schema.Coli.of(
                                        Schema.Primitive.BIGINT,
                                        Schema.Primitive.BOOLEAN)))));

        assertThat(Evolution.fromPath(List.of(), schema))
                .isEqualTo(Optional.of(schema));

        assertThat(Evolution.fromPath(List.of("a", "b", "*", "c"), schema)).isEqualTo(Optional.of(Schema.Coli.of(
                Schema.Primitive.BIGINT,
                Schema.Primitive.BOOLEAN)));

        assertThat(Evolution.fromPath(List.of("a", "b", "c"), schema)).isEqualTo(Optional.empty());
    }

    @Test
    void testMergeArrays() {
        var schema1 = Schema.Array.of(Schema.Primitive.NULL);
        var schema2 = Schema.Array.of(Schema.Primitive.BIGINT);

        var merged = Evolution.merge(schema1, schema2);

        assertThat(merged).isEqualTo(Schema.Array.of(
                Schema.Coli.of(
                        Schema.Primitive.NULL,
                        Schema.Primitive.BIGINT)));
    }

    @Test
    void testMergeArrayOfArrays() {
        var schema1 = Schema.Array.of(Schema.Array.of(Schema.Primitive.NULL));
        var schema2 = Schema.Array.of(Schema.Array.of(Schema.Primitive.BIGINT));

        var merged = Evolution.merge(schema1, schema2);

        assertThat(merged).isEqualTo(Schema.Array.of(
                Schema.Array.of(
                        Schema.Coli.of(
                                Schema.Primitive.NULL,
                                Schema.Primitive.BIGINT))));
    }

    @Test
    void testEqualWithArraySchema() {
        var schema1 = Schema.Array.of(Schema.Primitive.BIGINT);
        var schema2 = Schema.Array.of(Schema.Primitive.DOUBLE);

        assertThat(Evolution.equal(schema1, schema2)).isFalse();

        var schema3 = Schema.Array.of(Schema.Primitive.BOOLEAN);
        var schema4 = Schema.Array.of(Schema.Primitive.BOOLEAN);

        assertThat(Evolution.equal(schema3, schema4)).isTrue();
    }

    @Test
    void testEqualWithDictSchema() {
        var schema1 = Schema.Dict.of("name", Schema.Primitive.TEXT);
        var schema2 = Schema.Dict.of("id", Schema.Primitive.BIGINT);

        assertThat(Evolution.equal(schema1, schema2)).isFalse();

        var schema3 = Schema.Dict.of("age", Schema.Primitive.BIGINT);
        var schema4 = Schema.Dict.of("age", Schema.Primitive.BIGINT);

        assertThat(Evolution.equal(schema3, schema4)).isTrue();
    }

    @Test
    void testEqualWithColiSchema() {
        var schema1 = Schema.Coli.of(Schema.Primitive.TEXT, Schema.Primitive.BOOLEAN);
        var schema2 = Schema.Coli.of(Schema.Primitive.TEXT, Schema.Primitive.BIGINT);

        assertThat(Evolution.equal(schema1, schema2)).isFalse();

        var schema3 = Schema.Coli.of(Schema.Primitive.TEXT, Schema.Primitive.BOOLEAN);
        var schema4 = Schema.Coli.of(Schema.Primitive.TEXT, Schema.Primitive.BOOLEAN);

        assertThat(Evolution.equal(schema3, schema4)).isTrue();
    }

    @Test
    void testTryCast() {
        var schema1 = Schema.Coli.of(
                Schema.Primitive.TEXT,
                Schema.Array.of(Schema.Primitive.BOOLEAN));
        var value = List.of(true, Map.of("key", false), 666);
        var result = Evolution.tryCast(value, schema1);
        assertThat(result).isEqualTo(PartialValue.of(
                "[true, {key=false}, 666]",
                value));
    }

    @Test
    void testSimilar1() {
        Schema.I schema1 = Schema.Dict.of(
                "=", Schema.Primitive.DOUBLE,
                "smallint", Schema.Dict.of(
                        "lucky", Schema.Primitive.BIGINT,
                        "truth", Schema.Primitive.BOOLEAN),
                "name_@", Schema.Dict.of(
                        "lucky", Schema.Primitive.BIGINT,
                        "truth", Schema.Primitive.BOOLEAN),
                "name", Schema.Coli.of(
                        Schema.Primitive.TEXT,
                        Schema.Array.of(Schema.Primitive.BOOLEAN)),
                "name_timestamp without time zone", Schema.Dict.of(
                        "lucky", Schema.Primitive.BIGINT,
                        "truth", Schema.Primitive.BOOLEAN),
                ">=", Schema.Primitive.DOUBLE,
                "?", Schema.Primitive.DOUBLE);

        Schema.I schema2 = Schema.Dict.of(
                "=", Schema.Primitive.DOUBLE,
                "smallint", Schema.Dict.of(
                        "lucky", Schema.Primitive.BIGINT,
                        "truth", Schema.Primitive.BOOLEAN),
                "name_@", Schema.Dict.of(
                        "lucky", Schema.Primitive.BIGINT,
                        "truth", Schema.Primitive.BOOLEAN),
                "name", Schema.Primitive.TEXT,
                "name_timestamp without time zone", Schema.Dict.of(
                        "lucky", Schema.Primitive.BIGINT,
                        "truth", Schema.Primitive.BOOLEAN),
                ">=", Schema.Primitive.DOUBLE,
                "?", Schema.Primitive.DOUBLE);

        assertThat(Evolution.similar(schema1, schema2)).isTrue();
    }

    @Test
    void testExtractPartialOriginal() {
        var given = Map.of(
                "ok", "value",
                "name", PartialValue.of("666", 666),
                "list", List.of(
                        1,
                        PartialValue.of(2, "23"),
                        3
                ),
                "nested", PartialValue.of(null,
                        List.of(
                                PartialValue.of(List.of(1,2,3), "[1,2,3]")
                        ))
        );

        var result =  Evolution.extractNonCasted(given);
        assertThat(result).isEqualTo(Map.of(
                "name", 666,
                "list", new ArrayList(){{
                    add(null);
                    add("23");
                    add(null);
                }},
                "nested", List.of("[1,2,3]")
        ));
    }
}
