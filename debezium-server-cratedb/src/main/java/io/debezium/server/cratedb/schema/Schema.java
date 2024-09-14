/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;


import java.util.*;


public record Schema() {

    public static I of() {
        return Dict.of();
    }

    public enum Primitive implements I {
        BIGINT, BOOLEAN, TEXT
    }

    public sealed interface I permits Array, Bit, Coli, Dict, Primitive {
    }

    public record Array(I innerType) implements I {
        public static Array of(I i) {
            return new Array(i);
        }
    }

    public record Bit(Number size) implements I {

    }

    public record Dict(Map<Object, I> fields) implements I {
        public static Dict of() {
            return of(new LinkedHashMap<>());
        }

        public static Dict of(Map<Object, I> fields) {
            return new Dict(fields);
        }

        public static Dict of(Object... keyValues) {
            var fields = new LinkedHashMap<Object, I>();
            for (int i = 0; i < keyValues.length; i += 2) {
                fields.put(keyValues[i], (I) keyValues[i + 1]);
            }
            return of(fields);
        }
    }

    public record Coli(Set<I> set) implements I {
        public static Coli of(Set<I> set) {
            return new Coli(set);
        }

        public static Coli of(I a, I b) {
            var set  = new LinkedHashSet<I>();
            // FIXME: a or b can be Coli, flatMap them
            set.add(a);
            set.add(b);
            return of(set);
        }
    }
}
