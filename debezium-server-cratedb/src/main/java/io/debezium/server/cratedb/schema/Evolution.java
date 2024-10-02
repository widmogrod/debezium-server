/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Implementation of the schema evolution for CrateDB
 *
 * @author Gabriel Habryn
 */
public class Evolution {
    public static Pair<Schema.I, Object> fromObject(Schema.I schema, Object object) {
        if (object instanceof PartialValue partialValue) {
            return fromObject(schema, partialValue.normalised());
        }

        return switch (object) {
            case Long ignored -> switch (schema) {
                case Schema.Primitive.BIGINT -> Pair.of(schema, object);
                default -> fallbackToTryCast(schema, object);
            };
            case Integer ignored -> switch (schema) {
                case Schema.Primitive.BIGINT -> Pair.of(schema, object);
                default -> fallbackToTryCast(schema, object);
            };
            case Double ignored -> switch (schema) {
                case Schema.Primitive.DOUBLE -> Pair.of(schema, object);
                default -> fallbackToTryCast(schema, object);
            };
            case String ignored -> switch (schema) {
                case Schema.Primitive.TEXT -> Pair.of(schema, object);
                default -> fallbackToTryCast(schema, object);
            };
            case Boolean ignored -> switch (schema) {
                case Schema.Primitive.BOOLEAN -> Pair.of(schema, object);
                default -> fallbackToTryCast(schema, object);
            };
            case Date date -> switch (schema) {
                case Schema.Primitive.BIGINT -> Pair.of(schema, date.getTime());
                default -> fallbackToTryCast(schema, object);
            };
            case List list -> switch (schema) {
                case Schema.Array schemaList -> {
                    if (list.isEmpty()) {
                        yield Pair.of(schemaList, PartialValue.of(null, list));
                    }

                    var result = new ArrayList<>();
                    var originalType = schemaList.innerType();
                    var innerType = originalType;
                    for (var item : list) {
                        var resultPair = fromObject(innerType, item);
                        var resultSchema = resultPair.getLeft();
                        var resultObject = resultPair.getRight();

                        // build up understanding of schema
                        // object inside may have new fields that can be added
                        innerType = merge(innerType, resultSchema);

                        result.add(resultObject);
                    }

                    // if inner type is collision
                    // it's hard to represent such types in cratedb
                    // let's just make result array empty
                    // there is no ARRAY(IGNORED) type that would help to put into array elements of different types
                    // and when all elements of whole array are not of the type of first colliding type
                    // or if they cannot be casted to given type, then whole array should be removed
                    // or create Partial object, that will hold original data, transformed, giving some level of control
                    // to parent process to control what to do with partial objects
                    // partial object could hold casted values also

                    var finalType = Schema.Array.of(innerType);
                    var finalResult = tryCast(result, finalType);

                    yield Pair.of(finalType, finalResult);
                }
                default -> fallbackToTryCast(schema, object);
            };

            case Map map -> switch (schema) {
                case Schema.Dict schema1 -> {
                    var object2 = new LinkedHashMap<>();
                    // immutability wrap:
                    Map<Object, Schema.I> fields = new HashMap<>(schema1.fields());

                    for (var fieldName : map.keySet()) {
                        var fieldValue = map.get(fieldName);

                        // empty arrays, or null values
                        if (shouldSkipValue(fieldValue)) {
                            continue;
                        }

                        fieldName = normaliseFieldName(fieldName);
                        // find if fieldName exists in schema fields
                        if (fields.containsKey(fieldName)) {
                            var existingType = fields.get(fieldName);

                            var resultPair = fromObject(existingType, fieldValue);
                            var resultSchema = resultPair.getLeft();
                            var resultObject = resultPair.getRight();

                            var finalType = merge(existingType, resultSchema);
                            fields.put(fieldName, finalType);

                            // add field and value to return object
                            // under normalized field name
                            object2.put(fieldName, resultObject);
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
                default -> fallbackToTryCast(schema, object);
            };

            default -> throw new IllegalArgumentException(
                    "Unknown object match (%s, %s)"
                            .formatted(schema.getClass(), object.getClass()));
        };
    }

    public static Pair<Schema.I, Object> fallbackToTryCast(Schema.I schema, Object object) {
        var finalSchema = merge(schema, detectType(object));
        var finalValue = tryCast(object, finalSchema);
        return Pair.of(finalSchema, finalValue);
    }

    @SuppressWarnings("unchecked")
    public static boolean isDebeziumArrayDocument(Map x) {
        if (x.isEmpty()) {
            return false;
        }

        return x.keySet()
                .stream()
                .allMatch(key -> key instanceof String str && str.matches("_\\d+"));
    }

    @SuppressWarnings("unchecked")
    public static Object dedebeziumArrayDocuments(Object value) {
        return switch (value) {
            case null -> null;
            case Map map -> {
                if (isDebeziumArrayDocument(map)) {
                    yield  ((Map<Object, Object>) map).entrySet().stream()
                            .sorted(Comparator.comparing(entry -> entry.getKey().toString()))
                            .map((entry) -> dedebeziumArrayDocuments(entry.getValue()))
                            .collect(Collectors.toList());
                }

                var result = new LinkedHashMap<>();
                for (var fieldName : map.keySet()) {
                    result.put(fieldName, dedebeziumArrayDocuments(map.get(fieldName)));
                }
                yield result;
            }
            case List list -> {
                var result = new ArrayList<>();
                for (var element : list) {
                    result.add(dedebeziumArrayDocuments(element));
                }
                yield result;
            }

            default -> value;
        };
    }

    public static Object tryCast(Object value, Schema.I type) {
        if (value instanceof PartialValue partialValue) {
            return tryCast(partialValue.original(), type);
        }

        return switch (type) {
            case Schema.Primitive primitive -> switch (primitive) {
                case Schema.Primitive.BIGINT -> {
                    try {
                        yield Integer.parseInt(value.toString());
                    }
                    catch (NumberFormatException e) {
                        yield PartialValue.of(null, value);
                    }
                }
                case Schema.Primitive.DOUBLE -> {
                    try {
                        yield Double.parseDouble(value.toString());
                    }
                    catch (NumberFormatException e) {
                        yield PartialValue.of(null, value);
                    }
                }
                case Schema.Primitive.TEXT -> {
                    if (value instanceof String) {
                        yield value.toString();
                    }

                    yield PartialValue.of(value.toString(), value);
                }
                case Schema.Primitive.BOOLEAN -> {
                    if (value instanceof Boolean bool) {
                        yield bool;
                    }
                    else if (value instanceof String str) {
                        yield Boolean.parseBoolean(str);
                    }
                    yield PartialValue.of(null, value);
                }
                case TIMETZ -> {
                    // FIXME: this should be detection of type
                    yield value;
                }
                case NULL -> {
                    if (value == null) {
                        yield null;
                    }

                    yield PartialValue.of(null, value);
                }
            };

            case Schema.Array(Schema.I innerType) -> switch (value) {
                case List list -> {
                    // FIX ERROR: Dynamic nested arrays are not supported
                    // replace [[]] with [null]
                    if (list.isEmpty()) {
                        yield PartialValue.of(null, list);
                    }

                    var result = new ArrayList<>();
                    for (var element : list) {
                        result.add(tryCast(element, innerType));
                    }
                    yield result;
                }
                default -> PartialValue.of(null, value);
            };
            case Schema.Dict(Map<Object, Schema.I> fields) -> switch (value) {
                case Map map -> {
                    var result = new LinkedHashMap<>();
                    for (var fieldName : map.keySet()) {
                        Object fieldValue = map.get(fieldName);

                        if (fields.containsKey(fieldName)) {
                            var valueType = fields.get(fieldName);
                            fieldValue = tryCast(fieldValue, valueType);
                            if (fieldValue instanceof PartialValue) {
                                // if value is partial, we could try to add type suffix here!
                                fieldName = typeSuffix(fieldName, valueType, detectType(fieldValue));
                            }
                        }

                        result.put(fieldName, fieldValue);
                    }
                    yield result;
                }
                default -> PartialValue.of(null, value);
            };
            case Schema.Coli(Set<Schema.I> set) -> {
                var firstType = set.iterator().next();
                yield tryCast(value, firstType);
            }

            default -> throw new IllegalArgumentException("Unsupported casting " + type.getClass());
        };
    }

    public static boolean shouldSkipValue(Object fieldValue) {
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
            return str.replaceAll("\\[", "bkt_").replaceAll("\\]", "_bkt").replaceAll(";", "_semicolon_").replaceAll("\\.", "_dot_");
        }

        return fieldName;
    }

    public static Schema.I merge(Schema.I a, Schema.I b) {
        return switch (a) {
            case Schema.Array(Schema.I innerTypeA) -> switch (b) {
                case Schema.Array(Schema.I innerTypeB) -> Schema.Array.of(merge(innerTypeA, innerTypeB));
                default -> Schema.Coli.of(a, b);
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

                default -> Schema.Coli.of(a, b);
            };

            case Schema.Coli(Set<Schema.I> setA) -> switch (b) {
                case Schema.Coli(Set<Schema.I> setB) -> Schema.Coli.of(setA, setB);
                case Schema.Dict dict -> {
                    // if in collision exists dict, merge them
                    Set<Schema.I> set = new LinkedHashSet<>();
                    for (var entry : setA) {
                        if (entry instanceof Schema.Dict) {
                            set.add(merge(entry, dict));
                        }
                        else {
                            set.add(entry);
                        }
                    }
                    yield Schema.Coli.of(set);
                }
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
        var lastPart = fieldName.substring(lastIndex + 1);
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
            case Long ignored -> Schema.Primitive.BIGINT;
            case Double ignored -> Schema.Primitive.DOUBLE;
            case Boolean ignored -> Schema.Primitive.BOOLEAN;
            case Date ignored -> Schema.Primitive.BIGINT;
            case List of -> Schema.Array.of(of.isEmpty() ? Schema.Primitive.NULL : detectType(of.getFirst()));
            case Map ignored -> Schema.Dict.of();
            case PartialValue(Object ignored, Object original) -> detectType(original);
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

    public static boolean equal(Schema.I a, Schema.I b) {
        if (a instanceof Schema.Array aArray && b instanceof Schema.Array bArray) {
            return equal(aArray.innerType(), bArray.innerType());
        }

        if (a instanceof Schema.Dict aDict && b instanceof Schema.Dict bDict) {
            var fieldsA = aDict.fields();
            var fieldsB = bDict.fields();

            if (fieldsA.size() != fieldsB.size()) {
                return false;
            }

            for (var fieldA : fieldsA.entrySet()) {
                var aKey = fieldA.getKey();
                if (!fieldsB.containsKey(aKey)) {
                    return false;
                }

                var aValue = fieldA.getValue();
                var bValue = fieldsB.get(aKey);
                if (!equal(aValue, bValue)) {
                    return false;
                }
            }

            return true;
        }

        if (a instanceof Schema.Coli aColi && b instanceof Schema.Coli bColi) {
            var aSet = aColi.set();
            var bSet = bColi.set();
            if (aSet.size() != bSet.size()) {
                return false;
            }

            // check if positional elements are the same type
            Iterator<Schema.I> aSetIterator = aSet.iterator();
            Iterator<Schema.I> bSetIterator = bSet.iterator();

            while (aSetIterator.hasNext() && bSetIterator.hasNext()) {
                Schema.I aElement = aSetIterator.next();
                Schema.I bElement = bSetIterator.next();

                if (!aElement.equals(bElement)) {
                    return false;
                }
            }

            return true;
        }

        return a.equals(b);
    }

    public static boolean similar(Schema.I a, Schema.I b) {
        // a is reference value build by Evolution, and b can be a value build by inferSchema
        // b values can have less fields
        // b values may not have collision types on arrays
        // but if there is collision in a, b needs to have first element from collision matching
        if (a instanceof Schema.Array aArray && b instanceof Schema.Array bArray) {
            return similar(aArray.innerType(), bArray.innerType());
        }

        if (a instanceof Schema.Dict aDict && b instanceof Schema.Dict bDict) {
            var fieldsA = aDict.fields();
            var fieldsB = bDict.fields();

            if (fieldsA.size() < fieldsB.size()) {
                return false;
            }

            for (var fieldA : fieldsA.entrySet()) {
                var aKey = fieldA.getKey();
                var aValue = fieldA.getValue();
                if (!fieldsB.containsKey(aKey)) {
                    // but if aValue is collision, tolerate it
                    if (aValue instanceof Schema.Coli ||
                            (aValue instanceof Schema.Array arr && arr.innerType() instanceof Schema.Coli)) {
                        return true;
                    }
                    return false;
                }

                var bValue = fieldsB.get(aKey);
                if (!similar(aValue, bValue)) {
                    return false;
                }
            }

            return true;
        }

        if (a instanceof Schema.Coli aColi && b instanceof Schema.Coli bColi) {
            var aSet = aColi.set();
            var bSet = bColi.set();
            if (aSet.size() != bSet.size()) {
                return false;
            }

            // check if positional elements are the same type
            Iterator<Schema.I> aSetIterator = aSet.iterator();
            Iterator<Schema.I> bSetIterator = bSet.iterator();

            while (aSetIterator.hasNext() && bSetIterator.hasNext()) {
                Schema.I aElement = aSetIterator.next();
                Schema.I bElement = bSetIterator.next();

                if (!aElement.equals(bElement)) {
                    return false;
                }
            }

            return true;
        }

        // WEEK: assumption, if first element of collision is the same as the original type then they can be considered similar
        if (a instanceof Schema.Coli aColi) {
            var aSet = aColi.set();
            var aFirst = aSet.iterator().next();

            if (similar(aFirst, b)) {
                return true;
            }
        }
        else if (b instanceof Schema.Coli bColi) {
            var bSet = bColi.set();
            var bFirst = bSet.iterator().next();

            if (similar(a, bFirst)) {
                return true;
            }
        }

        return a.equals(b);
    }
}
