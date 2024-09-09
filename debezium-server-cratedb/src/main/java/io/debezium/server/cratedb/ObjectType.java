/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import java.util.HashMap;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;

final public class ObjectType extends HashMap<ColumnName, TypeCollision> implements ColumnType {
    public ObjectType() {
        super();
    }

    public static ObjectType of(ColumnName name, ColumnType type) {
        var result = new ObjectType();
        result.putColumnNameWithType(name, type);
        return result;
    }

    @Deprecated
    public ColumnInfo putColumnNameWithType(ColumnName columnName, ColumnType columnType) {
        return this
                .computeIfAbsent(columnName, k -> new TypeCollision())
                .putType(columnType, new ColumnInfo(false, columnName));
    }

    public Pair<ColumnType, ColumnInfo> putColumnNameWithType2(ColumnName columnName, ColumnType columnType) {
        return this
                .computeIfAbsent(columnName, k -> new TypeCollision())
                .putType2(columnType, new ColumnInfo(false, columnName));
    }

    @Deprecated
    public Optional<Pair<ObjectType, ColumnInfo>> tryGetColumnInfoOfObjectType(ColumnName columnName) {
        if (containsKey(columnName)) {
            return get(columnName).tryGetColumInfoOfObjectType();
        }

        return Optional.empty();
    }

    public Optional<Pair<ColumnType, ColumnInfo>> tryGetColumnInfoOf(ColumnName columnName, ColumnType columnType) {
        if (containsKey(columnName)) {
            return get(columnName).tryGetColumInfoOf(columnType);
        }

        return Optional.empty();
    }

    public Optional<Pair<ColumnType, ColumnInfo>> tryGetColumnNamed(ColumnName columnName) {
        for (var entry : entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            for (var tc : value.entrySet()) {
                var k = tc.getKey();
                var v = tc.getValue();
                if (v.columnName().equals(columnName)) {
                    return Optional.of(Pair.of(k, v));
                }
            }
        }

        return Optional.empty();
    }

    @Override
    public String shortName() {
        return "object";
    }

    @Override
    public void merge(ColumnType columnType) {
        if (columnType instanceof ObjectType other) {
            other.forEach((columnName, typeCollision) -> {
                if (this.containsKey(columnName)) {
                    // merge type collision
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
