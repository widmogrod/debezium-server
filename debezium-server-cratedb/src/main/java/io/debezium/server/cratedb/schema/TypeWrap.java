/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import java.sql.Time;
import java.util.*;

/**
 * Wrap incoming objects into DynamoDB style of documents.
 * Which allows to ensure type strict schema for CrateDB,
 * By expressing sum type as product type.
 * <p>
 * value = Int | String | Map
 * value = {I: _} | {S: _} | {"M": _}
 * <p>
 *
 * {S: _} – String
 * {I: } – Ingeger
 * {F: } - Float and Real
 * {B: } – Boolean
 * {M: {_}}: map
 * {L: [_]}: list
 *
 * @author Gabriel Habryn
 */
public class TypeWrap {
    public static Object wrap(Object value) {
        return switch (value) {
            case null -> Map.of("NULL", true);
            case Integer v -> Map.of("I", v);
            case Short v -> Map.of("I", v);
            case Long v -> Map.of("I", v);
            case Float v -> Map.of("F", v);
            case Double v -> Map.of("F", v);
            case Boolean b -> Map.of("B", b);
            case String s -> Map.of("S", s);
            case Byte b -> Map.of("BINARY", Base64.getEncoder().encodeToString(new byte[]{b}));
            case Character s -> Map.of("S", s.toString());
            case Time d -> Map.of("N", d.getTime());
            case Map<?, ?> map -> {
                var result = new LinkedHashMap<>();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    result.put(entry.getKey(), wrap(entry.getValue()));
                }
                yield Map.of("M", result);
            }
            case List<?> list -> {
                var result = new ArrayList<Object>();
                for (Object item : list) {
                    result.add(wrap(item));
                }
                yield Map.of("L", result);
            }
            default -> Map.of("UNKNOWN", value.toString());
        };
    }
}
