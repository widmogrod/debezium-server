/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class SchemaTest {

    @Test
    void testMethod() {
        var schema = Schema.of();
        var object = Map.of("name", "hello");

        var result = fromObject(schema, object);
        var schema1 = result.getLeft();
        var object1 = result.getRight();

        // first object should not be transformed
        assertThat(object1).isEqualTo(object);
        // schema must be immutable
        assertThat(schema).isEqualTo(Schema.Dict.of());
        // new schema must reflect input object structure
        assertThat(schema1).isEqualTo(
                Schema.Dict.of("name", Schema.Collision.ofTextNamed("name"))
        );
    }

    private Pair<Schema.I, Object> fromObject(Schema.I schema, Object object) {
        return switch (object) {
            case Map x -> {
                if (!(schema instanceof Schema.Dict)) {
                    throw new IllegalArgumentException();
                }

                var schema1 = (Schema.Dict) schema;

                var object2 = new HashMap<>();
                Map<Object, Schema.Collision> fields = new HashMap<>(schema1.fields());

                for (var fieldName : x.keySet()) {
                    var fieldValue = x.get(fieldName);
                    // find if fieldName exists in schema fields
                    if (fields.containsKey(fieldName)) {
                        // get types that this field has
                        var collision = fields.get(fieldName);
                        for (var collisionInfo : collision.set()) {
                            var knownType = collisionInfo.type();
                            // and try to match fieldValue against known types
                            var result = matchType(knownType, fieldValue);
                            // when there is new value save it in field name associated with given type
                            if (result.isPresent()) {
                                // and field and value to return object
                                object2.put(collisionInfo.fieldName(), result.get());
                                break;
                            }
                        }

                        // when field cannot be converted to known types
                        // detect field type, and add collision type
                        // TODO this part most likely should be recursive and it should use fromObject()
                        // this also means that Schema should be part of union type
                        var detectedType = detectType(fieldValue);

                        // TODO add also detecting if field name needs to be - type-suffix
                        // current implementation assume that orginal field name is used
                        var collisionInfo = Schema.Collision.Info.of(fieldName, detectedType);
                        collision.set().add(collisionInfo);

                        // and field and value to return object
                        // under normalized field name
                        object2.put(collisionInfo.fieldName(), fieldValue);
                    }
                    else {
                        // schema don't have field yet
                        // detect field type, and add collision type
                        var detectedType = detectType(fieldValue);
                        var collisionInfo = Schema.Collision.Info.of(fieldName, detectedType);
                        var collision = Schema.Collision.of(collisionInfo);

                        fields.put(fieldName, collision);

                        // and field and value to return object
                        // under normalized field name
                        object2.put(collisionInfo.fieldName(), fieldValue);
                    }
                }

                var schema2 = Schema.Dict.of(fields);

                yield Pair.of(schema2, object2);
            }

            default -> Pair.of(schema, object);
        };
    }

    private Optional<Object> matchType(Schema.I knownType, Object fieldValue) {
        return switch (knownType) {
            case Schema.Primitive.TEXT -> Optional.of(fieldValue.toString());
            default -> Optional.empty();
        };
    }

    private Schema.I detectType(Object fieldValue) {
        return switch (fieldValue) {
            case String x -> Schema.Primitive.TEXT;
            default -> throw new IllegalArgumentException("Unknown type: " + fieldValue.getClass());
        };
    }
}