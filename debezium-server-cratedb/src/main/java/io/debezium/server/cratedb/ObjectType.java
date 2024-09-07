/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import java.util.HashMap;

final public class ObjectType extends HashMap<ColumnName, TypeCollision> implements ColumnType {
    public ObjectType() {
        super();
    }

    public static ColumnType of(ColumnName name, ColumnType type) {
        return new ObjectType().mergeColumn(name, type);
    }

    public ObjectType mergeColumn(ColumnName columnName, ColumnType columnType) {
        this.computeIfAbsent(columnName, k -> new TypeCollision()).putType(columnType, new ColumnInfo(false, columnName));
        return this;
    }

    @Override
    public String shortName() {
        return "object";
    }

    @Override
    public void merge(ColumnType columnType) {
        if (columnType instanceof ObjectType) {
            ObjectType other = (ObjectType) columnType;
            other.forEach((columnName, typeCollision) -> {
                if (this.containsKey(columnName)) {
                    // merge type colision
                    this.get(columnName).mergeAll(typeCollision);
                }
                else {
                    // add column when not present
                    this.putIfAbsent(columnName, typeCollision);
                }
            });
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ObjectType;
    }

    @Override
    public int hashCode() {
        return 1; // return same hash value for all instances
    }
}
