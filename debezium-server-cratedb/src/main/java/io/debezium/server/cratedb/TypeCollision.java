/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import java.util.HashMap;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;

public class TypeCollision extends HashMap<ColumnType, ColumnInfo> {
    public TypeCollision() {
        super();
    }

    @Deprecated
    public ColumnInfo putType(ColumnType columnType, ColumnInfo columnInfo) {
        if (this.isEmpty()) {
            this.put(columnType, columnInfo);
            return columnInfo;
        }

        if (containsKey(columnType)) {
            // Merge the column type with the existing one
            for (var e : entrySet()) {
                var k = e.getKey();
                if (k.equals(columnType)) {
                    k.merge(columnType);
                    return e.getValue();
                }
            }
        }

        ColumnInfo nv = new ColumnInfo(
                columnInfo.primaryKey(),
                new ColumnName(columnInfo.columnName().columnName() + "_" + columnType.shortName()));
        this.put(columnType, nv);
        return nv;
    }

    public Pair<ColumnType, ColumnInfo> putType2(ColumnType columnType, ColumnInfo columnInfo) {
        if (this.isEmpty()) {
            this.put(columnType, columnInfo);
            return Pair.of(columnType, columnInfo);
        }

        if (containsKey(columnType)) {
            // Merge the column type with the existing one
            for (var e : entrySet()) {
                var k = e.getKey();
                if (k.equals(columnType)) {
                    k.merge(columnType);
                    return Pair.of(k, e.getValue());
                }
            }
        }

        ColumnInfo nv = new ColumnInfo(
                columnInfo.primaryKey(),
                new ColumnName(columnInfo.columnName().columnName() + "_" + columnType.shortName()));
        this.put(columnType, nv);
        return Pair.of(columnType, nv);
    }

    public void mergeAll(TypeCollision typeCollision) {
        typeCollision.forEach(this::putType);
    }

    @Deprecated
    public Optional<Pair<ObjectType, ColumnInfo>> tryGetColumInfoOfObjectType() {
        for (ColumnType columnType : keySet()) {
            if (columnType instanceof ObjectType objectType) {
                return Optional.of(Pair.of(objectType, get(columnType)));
            }
        }

        return Optional.empty();
    }

    public Optional<Pair<ColumnType, ColumnInfo>> tryGetColumInfoOf(ColumnType columnType) {
        if (containsKey(columnType)) {
            return Optional.of(Pair.of(columnType, get(columnType)));
        }

        return Optional.empty();
    }
}
