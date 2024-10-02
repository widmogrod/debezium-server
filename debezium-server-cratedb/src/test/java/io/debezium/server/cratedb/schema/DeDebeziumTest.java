/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.debezium.server.cratedb.schema.Evolution.dedebeziumArrayDocuments;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class DeDebeziumTest {
    @Test
    void testDebeziumArrayEncodingDocument() {
        // https://debezium.io/documentation/reference/stable/transformations/mongodb-event-flattening.html#mongodb-event-flattening-array-encoding
        var object01 = Map.of(
                "_0", 1,
                "_1", false,
                "_2", 3,
                "_3", 4,
                "_4", true);

        var result = dedebeziumArrayDocuments(object01);
        assertThat(result).isEqualTo(List.of(
                1,
                false,
                3,
                4,
                true
        ));
    }

    @Test
    void testDebeziumArrayEncodingDocumentComplex() {
        var object01 = Map.of(
                "_id", "66fc5f0b50740a3851180366",
                "obj", Map.of(
                        "id", 4,
                        "name_character", Map.of(
                                "_0", Map.of(
                                        "_0", Map.of(),
                                        "_1", Map.of(
                                                "truth", false,
                                                "lucky", 444),
                                        "_2", "Queen"),
                                "_1", Map.of(
                                        "_0", Map.of(),
                                        "_1", Map.of(
                                                "truth", false,
                                                "lucky", 444),
                                        "_2", "Queen")),
                        "role", "King",
                        "-=", -123.31239),
                "embeddedDoc", Map.of("x", 35),
                "array", Map.of("_0", 93, "_1", 93, "_2", 68)
        );

        var result = dedebeziumArrayDocuments(object01);

        assertThat(result).usingRecursiveComparison().isEqualTo(Map.of(
                "_id", "66fc5f0b50740a3851180366",
                "obj", Map.of(
                        "id", 4,
                        "name_character", List.of(
                                List.of(Map.of(), Map.of("lucky", 444, "truth", false), "Queen"),
                                List.of(Map.of(), Map.of("lucky", 444, "truth", false),  "Queen")
                        ),
                        "role", "King",
                        "-=", -123.31239),
                "embeddedDoc", Map.of("x", 35),
                "array", List.of(93, 93, 68)
        ));
    }
}
