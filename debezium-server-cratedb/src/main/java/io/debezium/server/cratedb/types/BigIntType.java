/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.types;

public record BigIntType() implements ColumnType {
    @Override
    public String shortName() {
        return "bigint";
    }

    @Override
    public void merge(ColumnType columnType) {
        // no-op
    }
}
