/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class SchemaTest {

    @Test
    void testFirstTransformation() {
        var schema = Schema.of();
        var object01 = Map.of(
                "name", "hello",
                "age", 1,
                "address", List.of(
                        Map.of("zip-code", "12-345")
                )
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
                                Map.of("zip-code", "12-345")
                        )
                )
        );
        // schema must be immutable
        assertThat(schema).isEqualTo(Schema.Dict.of());
        // new schema must reflect input object structure
        assertThat(schema1).isEqualTo(
                Schema.Dict.of(
                        "name", Schema.Collision.of(Schema.Collision.Info.textNamed("name")),
                        "age", Schema.Collision.of(Schema.Collision.Info.intNamed("age")),
                        "address", Schema.Collision.of(Schema.Collision.Info.of("address", Schema.Array.of(Schema.Dict.of(
                                "zip-code", Schema.Collision.of(
                                        Schema.Collision.Info.textNamed("zip-code")
                                )
                        ))))
                )
        );

        var object02 = Map.of(
                "name", false,
                "age", "not available",
                "address", List.of(
//                        Map.of("zip-code", List.of(false)),
                        Map.of("country", "Poland")
                )
        );
        var result2 = fromObject(schema1, object02);
        var schema2 = result2.getLeft();
        var object3 = result2.getRight();

        // object should be converted
        assertThat(object3).isEqualTo(
                Map.of(
                        "name", "false",
                        "age_text", "not available",
                        // FIXME: this is wrong prefix
                        "address_object_array", List.of(
//                                Map.of("zip-code", List.of(false)),
                                Map.of("country", "Poland")
                        )
                )
        );
        // schema must be immutable
        assertThat(schema1).isEqualTo(
                Schema.Dict.of(
                        "name", Schema.Collision.of(Schema.Collision.Info.textNamed("name")),
                        "age", Schema.Collision.of(Schema.Collision.Info.intNamed("age")),
                        "address", Schema.Collision.of(Schema.Collision.Info.of("address", Schema.Array.of(Schema.Dict.of(
                                "zip-code", Schema.Collision.of(
                                        Schema.Collision.Info.textNamed("zip-code")
                                )
                        ))))
                )
        );
        assertThat(schema2).isEqualTo(
                Schema.Dict.of(
                        "name", Schema.Collision.of(Schema.Collision.Info.textNamed("name")),
                        "age", Schema.Collision.of(
                                Schema.Collision.Info.intNamed("age"),
                                Schema.Collision.Info.textNamed("age_text")
                        ),
                        "address", Schema.Collision.of(
                                Schema.Collision.Info.of("address", Schema.Array.of(Schema.Dict.of(
                                        "zip-code", Schema.Collision.of(
                                                Schema.Collision.Info.textNamed("zip-code")
                                        )
                                ))),
                                // FIXME this is wrong collision resolution
                                Schema.Collision.Info.of("address_object_array", Schema.Array.of(Schema.Dict.of(
                                        "country", Schema.Collision.of(
                                                Schema.Collision.Info.textNamed("country")
                                        )
                                )))
                        )
                )
        );
    }

    private Pair<Schema.I, Object> fromObject(Schema.I schema, Object object) {
        return switch (object) {
            case Integer ignored -> switch (schema) {
                case Schema.Primitive.BIGINT -> Pair.of(schema, object);
                default -> throw new IllegalArgumentException("Unknown schema type: " + schema);
            };

            case Boolean ignored -> switch (schema) {
                case Schema.Primitive.BOOLEAN -> Pair.of(schema, object);
                default -> throw new IllegalArgumentException("Unknown schema type: " + schema);
            };

            case String ignored -> switch (schema) {
                case Schema.Primitive.TEXT -> Pair.of(schema, object);
                default -> throw new IllegalArgumentException("Unknown schema type: " + schema);
            };

            case List list -> switch (schema) {
                case Schema.Array schemaList -> {
                    var result = new ArrayList<>();
                    var innerType = schemaList.innerType();
                    for (var item : list) {
                        // assuming that list is of the same type
                        // we don't need to detect type per element
                        // FIXME: this is wrong assumption
//                        var detectedType = detectType(item);

                        var resultPair = fromObject(innerType, item);
                        var resultSchema = resultPair.getLeft();
                        var resultObject = resultPair.getRight();
                        result.add(resultObject);

                        // build up understanding of schema
                        // object inside may have new fields that can be added
                        innerType = resultSchema;
                    }

                    var schema2 = Schema.Array.of(innerType);

                    yield Pair.of(schema2, result);
                }
                default -> throw new IllegalArgumentException("Unknown schema type: " + schema);
            };

            case Map x -> {
                if (!(schema instanceof Schema.Dict)) {
                    throw new IllegalArgumentException();
                }

                var schema1 = (Schema.Dict) schema;

                var object2 = new HashMap<>();
                // immutability wrap:
                Map<Object, Schema.Collision> fields = new HashMap<>(schema1.fields());

                for (var fieldName : x.keySet()) {
                    var fieldValue = x.get(fieldName);
                    // find if fieldName exists in schema fields
                    if (fields.containsKey(fieldName)) {
                        boolean foundAndConverted = false;
                        // get types that this field has
                        var collision = new LinkedHashSet<>(fields.get(fieldName).set());
                        for (var collisionInfo : collision) {
                            var knownType = collisionInfo.type();
                            // and try to match fieldValue against known types
                            // FIXME: what if there is a type that can be used for explicit case?
                            //        conversion should happen when there is not 1:1 type cast
                            // FIXME: this conversion needs to be recursive
                            //        and part of values may not convert, or for result into other conflicts
                            //        one option how to solve it is
                            //        A - use fromObject and introduce merge function
                            //        B - don't convert types?
                            var result = tryConvertToType(knownType, fieldValue);
                            // when there is new value save it in field name associated with given type
                            if (result.isPresent()) {
                                // and field and value to return object
                                object2.put(collisionInfo.fieldName(), result.get());
                                foundAndConverted = true;
                                break;
                            }
                        }

                        if (foundAndConverted) {
                            continue;
                        }

                        // when field cannot be converted to known types
                        // detect field type, and add collision type
                        var detectedType = detectType(fieldValue);

                        var resultPair = fromObject(detectedType, fieldValue);
                        var resultSchema = resultPair.getLeft();
                        var resultObject = resultPair.getRight();

                        // introduce new collision type
                        var newFieldName = fieldSuffix(fieldName, resultSchema);
                        // make sure that other field names are not named same
                        newFieldName = uniqueField(newFieldName, collision);

                        // create new collision information
                        var collisionInfo = Schema.Collision.Info.of(newFieldName, resultSchema);
                        // and update schema with new information
                        collision.add(collisionInfo);
                        fields.put(fieldName, Schema.Collision.of(collision));

                        // add field and value to return object
                        // under normalized field name
                        object2.put(collisionInfo.fieldName(), resultObject);
                    }
                    else {
                        // schema don't have field yet
                        // detect field type, and add collision type
                        var detectedType = detectType(fieldValue);

                        var resultPair = fromObject(detectedType, fieldValue);
                        var resultSchema = resultPair.getLeft();
                        var resultObject = resultPair.getRight();

                        var collisionInfo = Schema.Collision.Info.of(fieldName, resultSchema);
                        var collision = Schema.Collision.of(collisionInfo);

                        fields.put(fieldName, collision);

                        // and field and value to return object
                        // under normalized field name
                        object2.put(collisionInfo.fieldName(), resultObject);
                    }
                }

                var schema2 = Schema.Dict.of(fields);

                yield Pair.of(schema2, object2);
            }

            default -> throw new IllegalArgumentException(
                    "Unknown object match (%s, %s)"
                            .formatted(schema.getClass(), object.getClass()));
        };
    }

    private Object uniqueField(Object fieldName, LinkedHashSet<Schema.Collision.Info> collision) {
        final Object[] newFieldName = {fieldName};
        int counter = 1;

        while (collision.stream().anyMatch(info -> info.fieldName().equals(newFieldName[0]))) {
            newFieldName[0] = fieldName + "_" + counter;
            counter++;

            if (counter > 20) {
                throw new IllegalArgumentException("Too many collisions in field naming.");
            }
        }

        return newFieldName[0];
    }

    private Object fieldSuffix(Object fieldName, Schema.I type) {
        return fieldName.toString() + "_" + suffixType(type);
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
        };
    }

    private Optional<Object> tryConvertToType(Schema.I knownType, Object fieldValue) {
        return switch (knownType) {
            case Schema.Primitive.TEXT -> switch (fieldValue) {
                case String ignored -> Optional.of(fieldValue);
                case Boolean ignored -> Optional.of(fieldValue.toString());
                case Integer ignored -> Optional.of(fieldValue.toString());
                case Long ignored -> Optional.of(fieldValue.toString());
                case Float ignored -> Optional.of(fieldValue.toString());
                case Double ignored -> Optional.of(fieldValue.toString());
                default -> Optional.empty();
            };

            case Schema.Primitive.BIGINT -> switch (fieldValue) {
                case Boolean ignored -> Optional.of((int) fieldValue);
                case Integer ignored -> Optional.of(fieldValue);
                case Long ignored -> Optional.of((int) fieldValue);
                case Float ignored -> Optional.of((int) fieldValue);
                case Double ignored -> Optional.of((int) fieldValue);
                default -> Optional.empty();
            };

            case Schema.Array(Schema.I innerType) -> switch (fieldValue) {
                case List list -> {
                    var result = new ArrayList<>();
                    for (var item : list) {
                        var newItem = tryConvertToType(innerType, item);
                        if (newItem.isPresent()) {
                            result.add(newItem);
                        }
                        else {
                            yield Optional.empty();
                        }
                        // FIXME: implicitly ignores elements here that cannot be converted
                        //        we have two options:
                        //        A - ignore elements;
                        //        B - return Option.empty() and infer fresh type
                        //          currently (easier) is option A

                    }

                    yield Optional.of(result);
                }

                default -> Optional.empty();
            };

            case Schema.Dict dict -> switch (fieldValue) {
//                case Map ignored -> {
//                    var result = new HashMap<>();
//                    for (var entry : result.entrySet()) {
//                        tryConvertToType();
//
//                    }
//
//                    yield Optional.of(result);
//                }

                default -> Optional.empty();
            };


            default -> throw new IllegalArgumentException("Unknown type: " + knownType);
        };
    }

    private Schema.I detectType(Object fieldValue) {
        return switch (fieldValue) {
            case String ignored -> Schema.Primitive.TEXT;
            case Integer ignored -> Schema.Primitive.BIGINT;
            case Boolean ignored -> Schema.Primitive.BOOLEAN;
            case List of -> Schema.Array.of(detectType(of.get(0)));
            case Map ignored -> Schema.Dict.of();
            default -> throw new IllegalArgumentException("Unknown type: " + fieldValue.getClass());
        };
    }
}