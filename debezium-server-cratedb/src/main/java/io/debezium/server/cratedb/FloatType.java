/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

public record FloatType() implements ColumnType {
    @Override
    public String shortName() {
        return "real";
    }

    @Override
    public void merge(ColumnType columnType) {
        // no-op
    }
}
