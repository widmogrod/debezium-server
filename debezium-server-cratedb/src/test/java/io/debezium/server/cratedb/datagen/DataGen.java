/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.datagen;

import java.util.List;
import java.util.Map;

/**
 * Data generation utils, to explore permutations of data
 *
 * @author Gabriel Habryn
 */
public class DataGen {
    static final public List<String> randomChar = List.of(
            "" +
            "[", ",", ".", "!", "@", "#", "$", "%", "^", "&", "*", "(", ")",
            "-", "+", "=", "{", "}", "[", "]", "|", "\\",
            ";", ":", "'", "\"", "<", ">", "?", "/", "~", "`");

    static final public List<String> shortTypeNames = List.of(
            "smallint", "bigint", "integer",
            "double precision", "real",
            "timestamp with time zone", "timestamp without time zone",
            "bit",
            "ip", "text",
            "object", "boolean", "character", "float_vector",
            "geo_point", "geo_shape");

    static public Object generateObject() {
        return Map.of(
                "id", 4,
                "role", "King",
                generateKey(), generateValue(),
                generateRandomChar(), -123.31239);
    }

    static public Object generateValue() {
        // random value
        var rand = Math.random();
        if (rand < 0.20) {
            return "Queen";
        }
        else if (rand < 0.40) {
            return 666;
        }
        else if (rand < 0.60) {
            return true;
        }
        else if (rand < 0.80) {
            return generateList();
        }
        else {
            return Map.of(
                    "truth", false,
                    "lucky", 444);
        }
    }

    static public String generateKey() {
        var rand = Math.random();
        if (rand < 0.25) {
            return "name";
        }
        else if (rand < 0.50) {
            return "name_" + generateShortTypeName();
        }
        else if (rand < 0.75) {
            return "name_" + generateRandomChar();
        }
        else {
            return generateShortTypeName();
        }
    }

    static public String generateRandomChar() {
        return randomChar.get((int) (Math.random() * randomChar.size()));
    }

    static public String generateShortTypeName() {
        return shortTypeNames.get((int) (Math.random() * shortTypeNames.size()));
    }

    static public List<Object> generateList() {
        var rand = Math.random();
        if (rand < 0.33) {
            return List.of(
                    generateValue(),
                    generateValue(),
                    generateValue());
        }
        else if (rand < 0.66) {
            var val = generateValue();
            return List.of(val, val);
        }
        else {
            return List.of();
        }
    }
}
