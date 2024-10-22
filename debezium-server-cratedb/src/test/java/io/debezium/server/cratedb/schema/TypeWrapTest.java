/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TypeWrapTest {
    @Test
    void testConversion() {
        var input = new ArrayList<>();
        input.add(Integer.valueOf("123"));
        input.add(Long.valueOf("12345678910"));
        input.add(Double.valueOf("123.45"));
        input.add(String.valueOf("Hello"));
        input.add(Character.valueOf('a'));
        input.add(Float.valueOf("123.45"));
        input.add(Short.valueOf("12345"));
        input.add(Boolean.valueOf("true"));
        input.add(Byte.valueOf("12"));
        input.add(Map.of(
                "a", 1,
                "b", List.of(
                        2, false, Map.of("c", "d"), List.of(List.of("x", "y", "z"))
                )
        ));

        var result = TypeWrap.wrap(input);
        assertThat(result).usingRecursiveComparison().isEqualTo(Map.of(
                "L", List.of(
                        Map.of("I", 123),
                        Map.of("I", 12345678910L),
                        Map.of("F", 123.45),
                        Map.of("S", "Hello"),
                        Map.of("S", "a"),
                        Map.of("F", 123.45f),
                        Map.of("I", Short.valueOf("12345")),
                        Map.of("B", true),
                        Map.of("BINARY", "DA=="),
                        Map.of("M", Map.of(
                                "a", Map.of("I", 1),
                                "b", Map.of("L", List.of(
                                        Map.of("I", 2),
                                        Map.of("B", false),
                                        Map.of("M", Map.of("c", Map.of("S", "d"))),
                                        Map.of("L", List.of(Map.of("L", List.of(
                                                Map.of("S", "x"),
                                                Map.of("S", "y"),
                                                Map.of("S", "z")
                                        ))))
                                ))
                        ))
                )
        ));
    }

}