/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import java.util.HashMap;

public class TypeCollision extends HashMap<ColumnType, ColumnInfo> {
    public TypeCollision() {
        super();
    }

    public void putType(ColumnType columnType, ColumnInfo columnInfo) {
        if (this.isEmpty()) {
            this.put(columnType, columnInfo);
            return;
        }


        if (containsKey(columnType)) {
            // Merge the column type with the existing one
            forEach((k, v) -> {
                if (k.equals(columnType)) {
                    k.merge(columnType);
                    this.put(k, v);
                }
            });
            return;
        }

        ColumnInfo nv = new ColumnInfo(
                columnInfo.primaryKey(),
                new ColumnName(columnInfo.columnName().columnName() + "_" + columnType.shortName()));
        this.put(columnType, nv);
    }

    public void mergeAll(TypeCollision typeCollision) {
        typeCollision.forEach(this::putType);
    }
}
