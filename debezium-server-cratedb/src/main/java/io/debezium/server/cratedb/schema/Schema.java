/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;


import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public record Schema() {

    public static I of() {
        return Dict.of();
    }

    public enum Primitive implements I {
        BIGINT, BOOLEAN, TEXT
    }

    public sealed interface I permits Primitive, Array, Bit, Dict {
    }

    public record Array(I innerType) implements I {
        public static Array of(I i) {
            return new Array(i);
        }
    }

    public record Bit(Number size) implements I {

    }

    public record Dict(Map<Object, Collision> fields) implements I {
        public static Dict of() {
            return of(new HashMap<>());
        }

        public static Dict of(Map<Object, Collision> fields) {
            return new Dict(fields);
        }

        public static Dict of(Object... keyValues) {
            var fields = new HashMap<Object, Collision>();
            for (int i = 0; i < keyValues.length; i += 2) {
                fields.put(keyValues[i], (Collision) keyValues[i + 1]);
            }
            return of(fields);
        }
    }

    public record Collision(Set<Info> set) {
        public static Collision ofTextNamed(String fieldName) {
            return of(Info.textNamed(fieldName));
        }

        public static Collision of(Set<Info> set) {
            return new Collision(set);
        }

        public static Collision of(Info... types) {
            return of(new LinkedHashSet<>(List.of(types)));
        }

        public record Info(I type, Object fieldName) {
            public static Info of(Object fieldName, I type) {
                return new Info(type, fieldName);
            }

            public static Info textNamed(Object fieldName) {
                return of(fieldName, Primitive.TEXT);
            }

            public static Info intNamed(Object fieldName) {
                return of(fieldName, Primitive.BIGINT);
            }
        }
    }
}
