/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonValue;

public record PartialValue(Object normalised, Object original) {
    public static PartialValue of(Object of, Object result) {
        return new PartialValue(of, result);
    }

    @JsonValue
    public Object serialize() {
        return normalised;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof PartialValue p) {
            return Objects.equals(normalised, p.normalised) && Objects.equals(original, p.original);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(normalised, original);
    }
}
