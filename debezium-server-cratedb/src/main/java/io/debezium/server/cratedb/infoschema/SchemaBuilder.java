/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.infoschema;

import io.debezium.server.cratedb.schema.Evolution;
import io.debezium.server.cratedb.schema.Schema;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * Use information about schema in CrateDB and create internal representation
 *
 * @author Gabriel Habryn
 */
public class SchemaBuilder {
    public static Schema.I fromInformationSchema(List<ColumnInfo> columns) {
        LinkedHashMap<Object, Schema.I> fields = new LinkedHashMap<>();

        for (ColumnInfo column : columns) {
            var fieldName = column.columnName();
            var fieldType = getColumnType(column);

            var details = column.columnDetails();
            if (details != null) {
                if (!details.path().isEmpty()) {
                    // traverse list in reverse order
                    var list = details.path();
                    for (var i = list.size() - 1; i >= 0; i--) {
                        fieldType = Schema.Dict.of(list.get(i), fieldType);
                    }
                    fieldName = details.name();
                }
            }

            if (fields.containsKey(fieldName)) {
                var fieldTypePrevious = fields.get(fieldName);
                var fieldTypeFinal = Evolution.merge(fieldTypePrevious, fieldType);
                fields.put(fieldName, fieldTypeFinal);
            }
            else {
                fields.put(fieldName, fieldType);
            }
        }

        return Schema.Dict.of(fields);
    }

    private static Schema.I getColumnType(ColumnInfo column) {
        return switch (column.dataType()) {
            case "smallint", "bigint", "integer" -> Schema.Primitive.BIGINT;
            case "double precision", "real" -> Schema.Primitive.DOUBLE;
            case "timestamp with time zone", "timestamp without time zone" -> Schema.Primitive.TIMETZ;
            case "bit" -> Schema.Bit.of(column.characterMaximumLength());
            case "ip", "text" -> Schema.Primitive.TEXT;
            case "object" -> Schema.Dict.of();
            case "boolean" -> Schema.Primitive.BOOLEAN;
//            case "character" -> new CharType(column.characterMaximumLength());
//            case "float_vector" -> new ArrayType(new FloatType());
//            case "geo_point", "geo_shape" -> new GeoShapeType();
            default -> {
                if (column.isArray()) {
                    yield Schema.Array.of(getColumnType(column.subArray()));
                }

                throw new IllegalArgumentException("Unknown data type: " + column.dataType());
            }
        };
    }
}
