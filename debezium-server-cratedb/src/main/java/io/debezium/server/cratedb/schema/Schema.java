/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public record Schema() {

    public static I of() {
        return Dict.of();
    }

    public enum Primitive implements I {
        BIGINT,
        TEXT
    }

    public sealed interface I permits Primitive, Array, Bit, Dict {
    }

    public record Array(I of) implements I {

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

        public static Dict of(String jey, Collision collision) {
            var fields = new HashMap<Object, Collision>();
            fields.put(jey, collision);
            return of(fields);
        }
    }

    public record Collision(Set<Info> set) {
        public static Collision of(Info collisionInfo) {
            return new Collision(Set.of(collisionInfo));
        }

        public static Collision ofTextNamed(String fieldName) {
            return of(Info.textNamed(fieldName));
        }

        public record Info(I type, Object fieldName) {
            public static Info of(Object fieldName, I type) {
                return new Info(type, fieldName);
            }

            public static Info textNamed(Object fieldName) {
                return of(fieldName, Primitive.TEXT);
            }

            ;
        }
    }
}
