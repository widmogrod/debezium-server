/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

class SchemaTest {

    @Test
    void testFirstTransformation() {
        var schema = Schema.of();
        var object01 = Map.of(
                "name", "hello",
                "age", 1,
                "address", List.of(
                        Map.of("zip-code", "12-345")),
                "list_of_list", List.of(List.of(false))
//                "bag", List.of(1, false, "??", 2, true, "!")
        );

        var result = fromObject(schema, object01);
        var schema1 = result.getLeft();
        var object1 = result.getRight();

        // first object should not be transformed
        assertThat(object1).isEqualTo(
                Map.of(
                        "name", "hello",
                        "age", 1,
                        "address", List.of(
                                Map.of("zip-code", "12-345")),
                        "list_of_list", List.of(List.of(false))
//                        "bag_int_array", List.of(1, 2),
//                        "bag_bool_array", List.of(false, true),
//                        "bag_test_array", List.of("??", "!")
                )
        );
        // schema must be immutable
        assertThat(schema).isEqualTo(Schema.Dict.of());
        // new schema must reflect input object structure
        assertThat(schema1).isEqualTo(
                Schema.Dict.of(
                        "name", Schema.Primitive.TEXT,
                        "age", Schema.Primitive.BIGINT,
                        "address", Schema.Array.of(Schema.Dict.of(
                                "zip-code", Schema.Primitive.TEXT)),
                        "list_of_list", Schema.Array.of(Schema.Array.of(Schema.Primitive.BOOLEAN))
//                        "bag", Schema.Array.of(Schema.Coli.of(
//                                Schema.Primitive.BIGINT,
//                                Schema.Primitive.BOOLEAN,
//                                Schema.Primitive.TEXT
//                        ))
                )
        );

        var object02 = Map.of(
                "name", false,
                "age", "not available",
                "address", List.of(
                        Map.of("zip-code", List.of(false)),
                        Map.of("country", "Poland")));
        var result2 = fromObject(schema1, object02);
        var schema2 = result2.getLeft();
        var object3 = result2.getRight();

        // object should be converted
        assertThat(object3).isEqualTo(
                Map.of(
                        "name_bool", false,
                        "age_text", "not available",
                        "address", List.of(
                                Map.of("zip-code_bool_array", List.of(false)),
                                Map.of("country", "Poland"))
//                        "bag", Schema.Array.of(Schema.Coli.of(
//                                Schema.Primitive.BIGINT,
//                                Schema.Primitive.BOOLEAN,
//                                Schema.Primitive.TEXT
//                        ))
                )
        );
        // schema must be immutable
        assertThat(schema1).isEqualTo(
                Schema.Dict.of(
                        "name", Schema.Primitive.TEXT,
                        "age", Schema.Primitive.BIGINT,
                        "address", Schema.Array.of(Schema.Dict.of(
                                "zip-code", Schema.Primitive.TEXT)),
                        "list_of_list", Schema.Array.of(Schema.Array.of(Schema.Primitive.BOOLEAN))
                )
        );
        assertThat(schema2).isEqualTo(
                Schema.Dict.of(
                        "name", Schema.Coli.of(Schema.Primitive.TEXT, Schema.Primitive.BOOLEAN),
                        "age", Schema.Coli.of(Schema.Primitive.BIGINT, Schema.Primitive.TEXT),
                        "address", Schema.Array.of(Schema.Dict.of(
                                "zip-code", Schema.Coli.of(Schema.Primitive.TEXT, Schema.Array.of(Schema.Primitive.BOOLEAN)),
                                "country", Schema.Primitive.TEXT)),
                        "list_of_list", Schema.Array.of(Schema.Array.of(Schema.Primitive.BOOLEAN))
                )
        );
    }

    private Pair<Schema.I, Object> fromObject(Schema.I schema, Object object) {
        return switch (object) {
            case Integer ignored -> switch (schema) {
                case Schema.Primitive.BIGINT -> Pair.of(schema, object);
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

            case Map x -> {
                if (!(schema instanceof Schema.Dict)) {
                    throw new IllegalArgumentException();
                }

                var schema1 = (Schema.Dict) schema;

                var object2 = new LinkedHashMap<>();
                // immutability wrap:
                Map<Object, Schema.I> fields = new HashMap<>(schema1.fields());

                for (var fieldName : x.keySet()) {
                    var fieldValue = x.get(fieldName);
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

            default -> throw new IllegalArgumentException(
                    "Unknown object match (%s, %s)"
                            .formatted(schema.getClass(), object.getClass()));
        };
    }

    private Object typeSuffix(Object fieldName, Schema.I resultSchema, Schema.I detectedType) {
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
                return fieldName.toString() + "_" + suffixType(detectedType);
            }
        }

        return fieldName;
    }

    private Schema.I merge(Schema.I a, Schema.I b) {
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
                        "Cannot merge pair (%s, %s)"
                                .formatted(a.getClass(), b.getClass())
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

    private String suffixType(Schema.I type) {
        return switch (type) {
            case Schema.Array array -> suffixType(array.innerType()) + "_array";
            case Schema.Bit bit -> "bit" + bit.size();
            case Schema.Dict dict -> "object";
            case Schema.Primitive primitive -> switch (primitive) {
                case BIGINT -> "int";
                case BOOLEAN -> "bool";
                case TEXT -> "text";
            };
            default -> throw new IllegalStateException("Unexpected value: " + type);
        };
    }

    private Schema.I detectType(Object fieldValue) {
        return switch (fieldValue) {
            case String ignored -> Schema.Primitive.TEXT;
            case Integer ignored -> Schema.Primitive.BIGINT;
            case Boolean ignored -> Schema.Primitive.BOOLEAN;
            // FIXME: poly-arrays
            case List of -> Schema.Array.of(detectType(of.get(0)));
            case Map ignored -> Schema.Dict.of();
            default -> throw new IllegalArgumentException("Unknown type: " + fieldValue.getClass());
        };
    }
}
