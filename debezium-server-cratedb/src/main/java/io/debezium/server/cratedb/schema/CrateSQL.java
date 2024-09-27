/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Takes two schemas as input and produce sequence of SQL commands
 * that need to be applied to database to reflect proper state of data.
 *
 * @author Gabriel Habryn
 */
public class CrateSQL {
    public static List<String> toSQL(String tableName, Schema.I beforeSchema, Schema.I afterSchema) {
        // find differences between schemas
        return switch (beforeSchema) {
            case Schema.Dict(Map<Object, Schema.I> fieldsBefore) -> switch (afterSchema) {
                case Schema.Dict(Map<Object, Schema.I> fieldsAfter) -> {
                    var result = new ArrayList<String>();
                    var nestedArrays = extractNestedObjectTypes(List.of(), beforeSchema, afterSchema);

                    for (var nested : nestedArrays.entrySet()) {
                        var columnPath = nested.getKey();
                        var columnType = nested.getValue();
                        var alter = "ALTER TABLE \"" + tableName + "\" ADD COLUMN \"" + sqlColumnName(columnPath) + "\" " + sqlColumnType(columnType);
                        result.add(alter);
                    }

                    yield result;
                }
                default -> List.of();
            };

            // CrateDB don't allow for type altering,
            // so when we don't match schemas we do nothing
            default -> List.of();
        };
    }

    private static Map<List<Object>, Schema.I> extractNestedObjectTypes(List<Object> path, Schema.I beforeSchema, Schema.I afterSchema) {
        return switch (beforeSchema) {
            case Schema.Dict(Map < Object, Schema.I > fieldsBefore) -> switch (afterSchema) {
                case Schema.Dict(Map < Object, Schema.I > fieldsAfter) -> {
                    Map<List<Object>, Schema.I> result = new HashMap<>();
                    for (var after : fieldsAfter.entrySet()) {
                        var afterField = after.getKey();
                        var afterValue = after.getValue();

                        var newPath = new ArrayList<>(path);
                        // get unique column name instead of field name
                        newPath.add(afterField);

                        Map<List<Object>, Schema.I> nestedArrays = null;
                        if (fieldsBefore.containsKey(afterField)) {
                            var beforeValue = fieldsBefore.get(afterField);
                            nestedArrays = extractNestedObjectTypes(newPath, beforeValue, afterValue);
                        }
                        else {
                            nestedArrays = extractNestedArrayTypes(newPath, afterValue);
                        }

                        result.putAll(nestedArrays);
                    }

                    yield result;
                }
                default -> new HashMap<>();
            };
            default -> new HashMap<>();
        };
    }

    public static Map<List<Object>, Schema.I> extractNestedArrayTypes(List<Object> path, Schema.I schema) {
        return switch (schema) {
            case Schema.Dict(Map < Object, Schema.I > fields) -> {
                Map<List<Object>, Schema.I> result = new HashMap<>();
                for (var entry : fields.entrySet()) {
                    var fieldName = entry.getKey();
                    var fieldValue = entry.getValue();

                    var newPath = new ArrayList<>(path);
                    // get unique column name instead of field name
                    newPath.add(fieldName);

                    var nested = extractNestedArrayTypes(newPath, fieldValue);
                    result.putAll(nested);
                }

                yield result;
            }

            case Schema.Array arr -> {
                var nested = extractNestedArrayTypes(new ArrayList<>(path), arr.innerType());
                nested.put(path, arr);
                yield nested;
            }
            // case Schema.Coli(Set<Schema.I> set) -> new HashMap() {{
            // var nested = extractNestedArrayTypes(new ArrayList<>(path), set.iterator().next());
            // nested.putAll(nested);
            //
            // putAll(nested);
            // }};
            default -> new HashMap<>();
        };
    }

    public static String sqlColumnName(List<Object> path) {
        if (path.size() > 1) {
            // format to pattern col1['col2']['col3']
            // skip first element
            return path.subList(1, path.size()).stream().map(Object::toString).reduce(path.get(0).toString(), (a, b) -> a + "['" + b + "']");
        }
        else {
            return path.get(0).toString();
        }
    }

    public static String sqlColumnType(Schema.I columnType) {
        return switch (columnType) {
            case Schema.Array(Schema.I innerType) -> "ARRAY(" + sqlColumnType(innerType) + ")";
            case Schema.Bit(Number size) -> "BIT(" + size + ")";
            case Schema.Coli(Set<Schema.I> set) -> {
                yield sqlColumnType(set.iterator().next());
                // "OBJECT(IGNORED)";
            }
            case Schema.Dict ignored -> "OBJECT(DYNAMIC)";
            case Schema.Primitive primitive -> switch (primitive) {
                case BIGINT -> "BIGINT";
                case DOUBLE -> "DOUBLE";
                case BOOLEAN -> "BOOLEAN";
                case TEXT -> "TEXT";
                case TIMETZ -> "TIMETZ";
                case NULL -> "NULL";
            };
        };
    }
}
