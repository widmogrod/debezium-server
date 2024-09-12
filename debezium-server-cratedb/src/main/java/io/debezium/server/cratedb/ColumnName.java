/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import java.util.List;
import java.util.Objects;

import io.debezium.server.cratedb.types.ColumnType;

public record ColumnName(String columnName) {
    public ColumnName {
        Objects.requireNonNull(columnName);
        columnName = columnName
                .replaceAll("\\[", "bkt_").
                replaceAll("\\]", "_bkt").
                replaceAll("\\.", "_dot_");
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

    public static ColumnName of(Object s) {
        return new ColumnName(s.toString());
    }

    public static ColumnName normalized(String s, ColumnType columnType) {
        var suffix = "_" + columnType.shortName();
        if (s.endsWith(suffix)) {
            String columnName = s.substring(0, s.length() - suffix.length());
            // make sure that there is no accidental column name of the type matching the column type
            if (!columnName.isEmpty()) {
                return new ColumnName(columnName);
            }
        }

        return new ColumnName(s);
    }


    private static final List<String> typeNames = List.of(
            "smallint", "bigint", "integer", "double precision", "real", "timestamp with time zone",
            "timestamp without time zone", "bit", "ip", "text", "object", "boolean", "character", "float_vector",
            "geo_point", "geo_shape"
    );

//    public ColumnName withoutType() {
//        String newName = columnName;
//        while (typeNames.stream().anyMatch(newName::endsWith)) {
//            if (newName.isEmpty()) {
//                return this;
//            }
//
//            var hasUnderscore = newName.lastIndexOf('_') == -1;
//            if (!hasUnderscore) {
//                return this;
//            }
//
//            newName = newName.substring(0, newName.lastIndexOf('_'));
//        }
//
//        if (newName.isEmpty()) {
//            return this;
//        }
//
//
//        return new ColumnName(newName);
//    }
}
