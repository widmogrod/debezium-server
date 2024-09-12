/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.types;

public record ArrayType(ColumnType elementType) implements ColumnType {
    @Override
    public String shortName() {
        return elementType.shortName() + "_array";
    }

    @Override
    public void merge(ColumnType columnType) {
        if (columnType instanceof ArrayType) {
            elementType.merge(((ArrayType) columnType).elementType);
        }
    }
}
