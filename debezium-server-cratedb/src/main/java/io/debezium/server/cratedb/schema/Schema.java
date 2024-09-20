/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public record Schema() {

    public sealed interface I permits Array, Bit, Coli, Dict, Primitive {
    }

    public static I of() {
        return Dict.of();
    }

    public enum Primitive implements I {
        BIGINT,
        DOUBLE,
        BOOLEAN,
        TEXT,
        TIMETZ
    }

    public record Array(I innerType) implements I {

    public static Array of(I i) {
        return new Array(i);
    }}

    public record Bit(Number size) implements I {
        public static Bit of(Number size) {
            return new Bit(size);
        }
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
            // flatMap elements of type Coli
            LinkedHashSet<I> newSet = set.stream().flatMap(e -> {
                if (e instanceof Coli c) {
                    return c.set().stream();
                }
                return Set.of(e).stream();
            }).collect(Collectors.toCollection(LinkedHashSet::new));

            return new Coli(newSet);
        }

    public static Coli of(I... list) {
            var set = new LinkedHashSet<I>(Arrays.stream(list).toList());
            return of(set);
        }

    public static I of(Set<I> setA, Set<I> setB) {
            var set = new LinkedHashSet<>(setA);
            set.addAll(setB);
            return of(set);
        }

    public static I of(Set<I> setA, I b) {
            var set = new LinkedHashSet<>(setA);
            set.add(b);
            return of(set);
        }
    }
}
