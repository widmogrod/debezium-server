/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class EvolutionTest {
    @Test
    void testFromPath() {
        var schema = Schema.Dict.of(
                "a", Schema.Dict.of(
                        "b", Schema.Array.of(Schema.Dict.of(
                                "c", Schema.Coli.of(
                                        Schema.Primitive.BIGINT,
                                        Schema.Primitive.BOOLEAN
                                )))));

        assertThat(Evolution.fromPath(List.of(), schema))
                .isEqualTo(Optional.of(schema));

        assertThat(Evolution.fromPath(List.of("a", "b", "*", "c"), schema)).isEqualTo(Optional.of(Schema.Coli.of(
                Schema.Primitive.BIGINT,
                Schema.Primitive.BOOLEAN
        )));

        assertThat(Evolution.fromPath(List.of("a", "b", "c"), schema)).isEqualTo(Optional.empty());
    }
}