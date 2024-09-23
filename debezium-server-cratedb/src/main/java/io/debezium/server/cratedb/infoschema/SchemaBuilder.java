/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.infoschema;

import io.debezium.server.cratedb.schema.Evolution;
import io.debezium.server.cratedb.schema.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Use information about schema in CrateDB and create internal representation
 *
 * @author Gabriel Habryn
 */
public class SchemaBuilder {
    public final static Map<String, Boolean> knownTypes = new HashMap<String, Boolean>() {{
        put("smallint", true);
        put("bigint", true);
        put("integer", true);
        put("double precision", true);
        put("real", true);
        put("ip", true);
        put("text", true);
        put("boolean", true);
        put("object", true);
        put("timestamp without time zone", true);
        put("timestamp with time zone", true);
    }};

    public static Schema.I fromInformationSchema(List<ColumnInfo> columns) {
        LinkedHashMap<Object, Schema.I> fields = new LinkedHashMap<>();

        for (ColumnInfo column : columns) {
            var fieldName = column.columnName();
            var fieldType = getColumnType(column);
            var details = column.columnDetails();

            if (details != null) {
                if (!details.path().isEmpty()) {
                    // traverse list in reverse order
                    var list = new ArrayList<String>() {{
                        add(details.name());
                        addAll(details.path());
                    }};

                    for (var i = list.size() - 1; i >= 0; i--) {
                        var fieldName2 = list.get(i);
                        var subPath = list.stream().limit(i + 1).toList();
                        if (list.size() - 1 != i) {
                            fieldType = Schema.Dict.of(fieldName2, fieldType);
                            continue;
                        }

                        // prepend subPath with detail.name()
                        var element = Evolution.fromPath(subPath, Schema.Dict.of(fields));
                        if (!element.isEmpty()) {
                            var fieldTypePrevious = element.get();
                            fieldType = Schema.Dict.of(fieldName2, fieldType);
                            fieldType = Evolution.merge(fieldTypePrevious, fieldType);
                        }
                        else {
                            // check unsuffixed name
                            var unsuffixed = Evolution.unsuffiedTypeName(fieldName2, fieldType);
                            var unsuffixedFieldName = unsuffixed.getLeft();
                            var unsuffixedFieldType = unsuffixed.getRight();

                            var unsuffixedSubPath = list.stream().limit(i).collect(Collectors.toCollection(ArrayList::new));
                            unsuffixedSubPath.add(unsuffixedFieldName);

                            var unsuffixedElement = Evolution.fromPath(unsuffixedSubPath, Schema.Dict.of(fields));
                            if (!unsuffixedElement.isEmpty()) {
                                var originalType = unsuffixedElement.get();

                                if (!Evolution.equal(originalType, unsuffixedFieldType)) {
                                    var fieldTypeMerged = Evolution.merge(originalType, unsuffixedFieldType);
                                    fieldType = Schema.Dict.of(unsuffixedFieldName, fieldTypeMerged);
                                }
                                else {
                                    fieldType = Schema.Dict.of(fieldName2, fieldType);
                                }
                            }
                            else {
                                fieldType = Schema.Dict.of(fieldName2, fieldType);
                            }
                        }
                    }
                    fieldName = details.name();
                    fieldType = Evolution.fromPath(List.of(details.name()), fieldType).get();
                }
            }

            // undo suffixType from field name
            // and check if field without type name exists
            // if field exists, and has different type merge it as Collision type
            // otherwise treat it as new field

            if (fields.containsKey(fieldName)) {
                var fieldTypePrevious = fields.get(fieldName);
                var fieldTypeFinal = Evolution.merge(fieldTypePrevious, fieldType);
                fields.put(fieldName, fieldTypeFinal);
            }
            else {
                var unsuffixed = Evolution.unsuffiedTypeName(fieldName, fieldType);
                var unsuffixedFieldName = unsuffixed.getLeft();
                var unsuffixedFieldType = unsuffixed.getRight();
                if (fields.containsKey(unsuffixedFieldName)) {
                    var originalType = fields.get(unsuffixedFieldName);

                    if (!Evolution.equal(originalType, unsuffixedFieldType)) {
                        // accidental type naming
                        var fieldTypeMerged = Evolution.merge(originalType, unsuffixedFieldType);
                        fields.put(unsuffixedFieldName, fieldTypeMerged);
                    }
                    else {
                        fields.put(fieldName, fieldType);
                    }
                }
                else {
                    fields.put(fieldName, fieldType);
                }
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
