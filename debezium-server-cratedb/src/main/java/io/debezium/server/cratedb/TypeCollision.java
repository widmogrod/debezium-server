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

    public ColumnInfo putType(ColumnType columnType, ColumnInfo columnInfo) {
        if (this.isEmpty()) {
            this.put(columnType, columnInfo);
            return columnInfo;
        }

        if (containsKey(columnType)) {
            // Merge the column type with the existing one
            forEach((k, v) -> {
                if (k.equals(columnType)) {
                    k.merge(columnType);
                    this.put(k, v);
                }
            });
            return get(columnType);
        }

        ColumnInfo nv = new ColumnInfo(
                columnInfo.primaryKey(),
                new ColumnName(columnInfo.columnName().columnName() + "_" + columnType.shortName()));
        this.put(columnType, nv);
        return nv;
    }

    public void mergeAll(TypeCollision typeCollision) {
        typeCollision.forEach(this::putType);
    }

    public Optional<Pair<ObjectType, ColumnInfo>> getObjectType() {
        for (ColumnType columnType : keySet()) {
            if (columnType instanceof ObjectType objectType) {
                return Optional.of(Pair.of(objectType, get(columnType)));
            }
        }

        return Optional.empty();
    }
}
