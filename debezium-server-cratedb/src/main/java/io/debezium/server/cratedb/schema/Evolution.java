/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

/**
 * Implementation of the schema evolution for CrateDB
 *
 * @author Gabriel Habryn
 */
public class Evolution {
    public static Pair<Schema.I, Object> fromObject(Schema.I schema, Object object) {
        return switch (object) {
            case Integer ignored -> switch (schema) {
                case Schema.Primitive.BIGINT -> Pair.of(schema, object);
                default -> Pair.of(merge(schema, detectType(object)), object);
            };
            case Double ignored -> switch (schema) {
                case Schema.Primitive.DOUBLE -> Pair.of(schema, object);
                default -> Pair.of(merge(schema, detectType(object)), object);
            };
            case String ignored -> switch (schema) {
                case Schema.Primitive.TEXT -> Pair.of(schema, object);
                default -> Pair.of(merge(schema, detectType(object)), object);
            };
            case Boolean ignored -> switch (schema) {
                case Schema.Primitive.BOOLEAN -> Pair.of(schema, object);
                default -> Pair.of(merge(schema, detectType(object)), object);
            };

            case List list -> switch (schema) {
                case Schema.Array schemaList -> {
                    var result = new ArrayList<>();
                    var innerType = schemaList.innerType();
                    for (var item : list) {
                        var resultPair = fromObject(innerType, item);
                        var resultSchema = resultPair.getLeft();
                        var resultObject = resultPair.getRight();

                        // build up understanding of schema
                        // object inside may have new fields that can be added
                        innerType = merge(innerType, resultSchema);

                        result.add(resultObject);
                    }

                    yield Pair.of(Schema.Array.of(innerType), result);
                }
                default -> Pair.of(merge(schema, detectType(object)), object);
            };

            case Map x -> switch (schema) {
                case Schema.Dict schema1 -> {
                    var object2 = new LinkedHashMap<>();
                    // immutability wrap:
                    Map<Object, Schema.I> fields = new HashMap<>(schema1.fields());

                    for (var fieldName : x.keySet()) {
                        var fieldValue = x.get(fieldName);

                        // empty arrays, or null values
                        if (shouldShipValue(fieldValue)) {
                            continue;
                        }

                        fieldName = normaliseFieldName(fieldName);
                        // find if fieldName exists in schema fields
                        if (fields.containsKey(fieldName)) {
                            var existingType = fields.get(fieldName);

                            // detect field type, and add collision type
                            var detectedType = detectType(fieldValue);

                            var resultPair = fromObject(existingType, fieldValue);
                            var resultSchema = resultPair.getLeft();
                            var resultObject = resultPair.getRight();

                            var finalType = merge(existingType, resultSchema);
                            fields.put(fieldName, finalType);

                            var finalFieldName = typeSuffix(fieldName, finalType, detectedType);

                            // add field and value to return object
                            // under normalized field name
                            object2.put(finalFieldName, resultObject);
                        }
                        else {
                            // schema don't have field yet
                            // detect field type, and add collision type
                            var detectedType = detectType(fieldValue);

                            var resultPair = fromObject(detectedType, fieldValue);
                            var resultSchema = resultPair.getLeft();
                            var resultObject = resultPair.getRight();

                            fields.put(fieldName, resultSchema);

                            // and field and value to return object
                            // under normalized field name
                            object2.put(fieldName, resultObject);
                        }
                    }

                    yield Pair.of(Schema.Dict.of(fields), object2);
                }
                default -> Pair.of(merge(schema, detectType(object)), object);
            };

            default -> throw new IllegalArgumentException(
                    "Unknown object match (%s, %s)"
                            .formatted(schema.getClass(), object.getClass()));
        };
    }

    public static boolean shouldShipValue(Object fieldValue) {
        if (fieldValue == null) {
            return true;
        }

        if (fieldValue instanceof List list && list.isEmpty()) {
            return true;
        }

        return false;
    }

    public static Object normaliseFieldName(Object fieldName) {
        if (fieldName instanceof String str) {
            return str.
                    replaceAll("\\[", "bkt_").
                    replaceAll("\\]", "_bkt").
                    replaceAll(";", "_semicolon_").
                    replaceAll("\\.", "_dot_");
        }

        return fieldName;
    }

    public static Object sanitizeData(Schema.I schema, Object data) {
        // looks for patterns such as
        // - poly array: stored in object ignore, need to wrap type into {"__malformed": <original value>}
        // - empty array: filter out
        // - empty fields: filter out

        return switch (schema) {
            case Schema.Dict(Map<Object, Schema.I> fields) -> switch (data) {
                case Map map -> {
                    var result = new LinkedHashMap<>();
                    for (Map.Entry<Object, Schema.I> entry : fields.entrySet()) {
                        if (entry instanceof List<?> list && list.isEmpty()) {
                            continue;
                        }

                        result.put(entry.getKey(), normaliseFieldName(map.get(entry.getKey())));
                    }

                    yield result;
                }
                default -> data;
            };
            case Schema.Array(Schema.I innerType) -> switch (innerType) {
                case Schema.Coli ignored -> Map.of("__malformed", data);
                case Schema.Primitive.NULL -> null;
                default -> List.of(sanitizeData(innerType, data));
            };
            case Schema.Coli ignored -> Map.of("__malformed", data);
            case Schema.Primitive.NULL -> null;
            default -> data;
        };
    }

    public static Schema.I merge(Schema.I a, Schema.I b) {
        return switch (a) {
            case Schema.Array(Schema.I innerTypeA) -> switch (b) {
                case Schema.Array(Schema.I innerTypeB) -> Schema.Array.of(merge(innerTypeA, innerTypeB));
                default -> throw new IllegalArgumentException(
                        "Cannot merge pair (%s, %s)"
                                .formatted(a.getClass(), b.getClass())
                );
            };
            case Schema.Dict(Map<Object, Schema.I> fieldsA) -> switch (b) {
                case Schema.Dict(Map<Object, Schema.I> fieldsB) -> {
                    var fields = new HashMap<>(fieldsA);
                    for (var entry : fieldsB.entrySet()) {
                        // if field exists merge collisions
                        // otherwise put collision from B
                        var fieldName = entry.getKey();
                        var collisionB = entry.getValue();
                        if (fields.containsKey(fieldName)) {
                            var collisionA = fields.get(fieldName);
                            fields.put(fieldName, merge(collisionA, collisionB));
                        }
                        else {
                            fields.put(fieldName, collisionB);
                        }
                    }

                    yield Schema.Dict.of(fields);
                }

                default -> throw new IllegalArgumentException(
                        "Cannot merge pair (%s, %s as %s) \n%s\n%s\n"
                                .formatted(a.getClass(), b.getClass(), b, a, b)
                );
            };

            case Schema.Coli(Set<Schema.I> setA) -> switch (b) {
                case Schema.Coli(Set<Schema.I> setB) -> Schema.Coli.of(setA, setB);
                default -> Schema.Coli.of(setA, b);
            };

            default -> {
                if (a == b) {
                    yield a;
                }

                if (b instanceof Schema.Coli coli) {
                    yield merge(coli, a);
                }

                yield Schema.Coli.of(a, b);
            }
        };
    }

    public static Object typeSuffix(Object fieldName, Schema.I resultSchema, Schema.I detectedType) {
        if (resultSchema != detectedType) {
            if (resultSchema instanceof Schema.Coli coli) {
                var first = coli.set().stream().findFirst();
                // when first type of collision is the same as detectedType,
                // then means that that's an original type
                if (first.isPresent()) {
                    return typeSuffix(fieldName, first.get(), detectedType);
                }
            }
            else if (resultSchema instanceof Schema.Array aArray && detectedType instanceof Schema.Array bArray) {
                var result = typeSuffix(fieldName, aArray.innerType(), bArray.innerType());
                if (result != fieldName) {
                    return result + "_array";
                }
            }
            else if (!(resultSchema instanceof Schema.Dict && detectedType instanceof Schema.Dict)) {
                return fieldName.toString() + "_" + typeSuffix(detectedType);
            }
        }

        return fieldName;
    }

    public static String typeSuffix(Schema.I type) {
        return switch (type) {
            case Schema.Array(Schema.I innerType) -> typeSuffix(innerType) + "_array";
            case Schema.Bit(Number size) -> "bit" + size;
            case Schema.Dict ignored -> "object";
            case Schema.Primitive primitive -> switch (primitive) {
                case BIGINT -> "int";
                case DOUBLE -> "double";
                case BOOLEAN -> "bool";
                case TEXT -> "text";
                case TIMETZ -> "tz";
                case NULL -> "null";
            };
            case Schema.Coli ignored -> "collision";
        };
    }

    public static Pair<String, Schema.I> unsuffiedTypeName(String fieldName, Schema.I schema) {
        if (!fieldName.contains("_")) {
            return Pair.of(fieldName, schema);
        }

        var lastIndex = fieldName.lastIndexOf("_");
        var lastPart = fieldName.substring(lastIndex+1);
        var reminder = fieldName.substring(0, lastIndex);
        return switch (lastPart) {
            case "array" -> {
                var result = unsuffiedTypeName(reminder, schema);
                yield Pair.of(result.getLeft(), Schema.Array.of(result.getRight()));
            }

            case "int" -> Pair.of(reminder, Schema.Primitive.BIGINT);
            case "double" -> Pair.of(reminder, Schema.Primitive.DOUBLE);
            case "bool" -> Pair.of(reminder, Schema.Primitive.BOOLEAN);
            case "text" -> Pair.of(reminder, Schema.Primitive.TEXT);
            case "tz" -> Pair.of(reminder, Schema.Primitive.TIMETZ);
            case "null" -> Pair.of(reminder, Schema.Primitive.NULL);
            case "object" -> Pair.of(reminder, Schema.Dict.of());
            case "collision" -> Pair.of(reminder, Schema.Coli.of());
            default -> {
                // if starts with "bit" and rest is parsed to number
                // then extract number
                if (lastPart.startsWith("bit") && lastPart.substring(3).matches("\\d+")) {
                    int length = Integer.parseInt(lastPart.substring(3));
                    yield Pair.of(reminder, Schema.Bit.of(length));
                }
                else {
                    yield Pair.of(fieldName, schema);
                }
            }
        };
    }

    public static Schema.I detectType(Object fieldValue) {
        return switch (fieldValue) {
            case String ignored -> Schema.Primitive.TEXT;
            case Integer ignored -> Schema.Primitive.BIGINT;
            case Double ignored -> Schema.Primitive.DOUBLE;
            case Boolean ignored -> Schema.Primitive.BOOLEAN;
            case List of -> Schema.Array.of(of.isEmpty() ? Schema.Primitive.NULL : detectType(of.getFirst()));
            case Map ignored -> Schema.Dict.of();
            default -> throw new IllegalArgumentException("Unknown type: " + fieldValue.getClass());
        };
    }

    /**
     * Return schema under a path or none
     */
    public static Optional<Schema.I> fromPath(List<String> path, Schema.I schema) {
        if (path.isEmpty()) {
            return Optional.of(schema);
        }

        return switch (schema) {
            case Schema.Dict(Map<Object, Schema.I> fields) -> {
                var fieldName = path.get(0);
                if (fields.containsKey(fieldName)) {
                    var fieldValue = fields.get(fieldName);
                    var remainingPath = path.subList(1, path.size());
                    yield fromPath(remainingPath, fieldValue);
                }

                yield Optional.empty();
            }

            case Schema.Array(Schema.I innerType) -> {
                var fieldName = path.get(0);
                if (Objects.equals(fieldName, "*")) {
                    yield fromPath(path.subList(1, path.size()), innerType);
                }

                yield Optional.empty();
            }
            default -> Optional.empty();
        };
    }
}
