/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import java.util.Objects;

public record ColumnName(String columnName) {
    public ColumnName {
        Objects.requireNonNull(columnName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ColumnName that = (ColumnName) obj;

        return columnName.equals(that.columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }

    public static ColumnName normalized(String s, ColumnType columnType) {
        if (s.endsWith("_" + columnType.shortName())) {
            String columnName = s.substring(0, s.length() - columnType.shortName().length() - 1);
            // make sure that there is no accidental column name of the type matching the column type
            if (!columnName.isEmpty()) {
                return new ColumnName(s.substring(0, s.length() - columnType.shortName().length() -1));
            }
        }

        return new ColumnName(s);
    }
}
