/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.server.cratedb.schema.Evolution.fromObject;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class SchemaTest {
    @Test
    void mixedArrays() {
        // [[666, 666], [], Queen]
        var schema0 = Schema.Dict.of();
        var object0 = Map.of(
                "name", List.of(List.of(), List.of(666, 666), "Queen"));

        var result = fromObject(schema0, object0);
        var schema1 = result.getLeft();
        var object1 = result.getRight();

        assertThat(schema1).isEqualTo(Schema.Dict.of(
                "name", Schema.Array.of(
                        Schema.Coli.of(
                                Schema.Array.of(Schema.Coli.of(Schema.Primitive.BIGINT, Schema.Primitive.NULL)),
                                Schema.Primitive.TEXT))));
        assertThat(object1).isEqualTo(Map.of(
                "name", List.of(
                        PartialValue.of(null, List.of()),
                        List.of(PartialValue.of(null, 666), PartialValue.of(null, 666)),
                        PartialValue.of(null, "Queen"))));
    }

    @Test
    void testNormalizationOfFieldNames() {
        var schema = Schema.Dict.of();
        var object01 = Map.of(
                "name.", 1,
                "name[]", 2,
                "name{}", 3,
                "name:", 4,
                "name;", 5);

        var result = fromObject(schema, object01);
        var object1 = result.getRight();

        assertThat(object1).isEqualTo(Map.of(
                "name_dot_", 1,
                "namebkt__bkt", 2,
                "name{}", 3,
                "name:", 4,
                "name_semicolon_", 5));
    }

    @Test
    void testFirstTransformation() {
        var schema = Schema.Dict.of();
        var object01 = Map.of(
                "name", "hello",
                "age", 1,

                "address", List.of(
                        Map.of("zip-code", "12-345")),
                "list_of_list", List.of(List.of(false)),
                "bag", List.of(1, false, "??", 2, true, "!"));

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
                        "list_of_list", List.of(List.of(false)),
                        "bag", List.of(1, PartialValue.of(null, false),
                                PartialValue.of(null, "??"), 2,
                                PartialValue.of(null, true),
                                PartialValue.of(null, "!"))));
        // schema must be immutable
        assertThat(schema).isEqualTo(Schema.Dict.of());
        // new schema must reflect input object structure
        assertThat(schema1).isEqualTo(
                Schema.Dict.of(
                        "name", Schema.Primitive.TEXT,
                        "age", Schema.Primitive.BIGINT,
                        "address", Schema.Array.of(Schema.Dict.of(
                                "zip-code", Schema.Primitive.TEXT)),
                        "list_of_list", Schema.Array.of(Schema.Array.of(Schema.Primitive.BOOLEAN)),
                        "bag", Schema.Array.of(Schema.Coli.of(
                                Schema.Primitive.BIGINT,
                                Schema.Primitive.BOOLEAN,
                                Schema.Primitive.TEXT))));

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
        assertThat(object3).usingRecursiveComparison().isEqualTo(
                Map.of(
                        "name", PartialValue.of("false", false),
                        "age", PartialValue.of(null, "not available"),
                        "address", List.of(
                                Map.of("zip-code_bool_array", PartialValue.of("[false]", List.of(false))),
                                Map.of("country", "Poland"))));
        // schema must be immutable
        assertThat(schema1).isEqualTo(
                Schema.Dict.of(
                        "name", Schema.Primitive.TEXT,
                        "age", Schema.Primitive.BIGINT,
                        "address", Schema.Array.of(Schema.Dict.of(
                                "zip-code", Schema.Primitive.TEXT)),
                        "list_of_list", Schema.Array.of(Schema.Array.of(Schema.Primitive.BOOLEAN)),
                        "bag", Schema.Array.of(Schema.Coli.of(
                                Schema.Primitive.BIGINT,
                                Schema.Primitive.BOOLEAN,
                                Schema.Primitive.TEXT))));
        assertThat(schema2).isEqualTo(
                Schema.Dict.of(
                        "name", Schema.Coli.of(Schema.Primitive.TEXT, Schema.Primitive.BOOLEAN),
                        "age", Schema.Coli.of(Schema.Primitive.BIGINT, Schema.Primitive.TEXT),
                        "address", Schema.Array.of(Schema.Dict.of(
                                "zip-code", Schema.Coli.of(Schema.Primitive.TEXT, Schema.Array.of(Schema.Primitive.BOOLEAN)),
                                "country", Schema.Primitive.TEXT)),
                        "list_of_list", Schema.Array.of(Schema.Array.of(Schema.Primitive.BOOLEAN)),
                        "bag", Schema.Array.of(Schema.Coli.of(
                                Schema.Primitive.BIGINT,
                                Schema.Primitive.BOOLEAN,
                                Schema.Primitive.TEXT))));
    }

    @Test
    void testTypeSuffix() throws JsonProcessingException {
        var schema0 = Schema.Dict.of();
        var object0 = new HashMap<String, Object>() {{
            put("id", 8);
            put("descr", null);
            put("new_col", "hello world");
        }};

        var result = fromObject(schema0, object0);
        var schema1 = result.getLeft();
        var object00 = result.getRight();
        assertThat(schema1).isEqualTo(Schema.Dict.of(
                "id", Schema.Primitive.BIGINT,
                "new_col", Schema.Primitive.TEXT
        ));
        assertThat(object00).usingRecursiveAssertion().isEqualTo(new HashMap<String, Object>() {{
            put("id", 8);
            put("new_col", "hello world");
        }});

        var object1 = new HashMap<String, Object>() {{
            put("id", 8);
            put("descr", null);
            put("new_col", 88);
        }};
        var result2 = fromObject(schema1, object1);
        var schema2 = result2.getLeft();
        var object11 = result2.getRight();

        assertThat(schema2).isEqualTo(Schema.Dict.of(
                "id", Schema.Primitive.BIGINT,
                "new_col", Schema.Coli.of(
                        Schema.Primitive.TEXT,
                        Schema.Primitive.BIGINT
                )
        ));
        assertThat(object11).usingRecursiveAssertion().isEqualTo(new HashMap<String, Object>() {{
            put("id", 8);
            put("new_col", PartialValue.of("88", 88));
        }});

        var json11 = new ObjectMapper().writeValueAsString(object11);
        assertThat(json11).isEqualTo("{\"new_col\":\"88\",\"id\":8}");

        var object2 = new HashMap<String, Object>() {{
            put("id", 8);
            put("descr", null);
            put("new_col", List.of(1.1, 2.2, 3.3));
        }};
        var result3 = fromObject(schema2, object2);
        var schema3 = result3.getLeft();
        var object22 = result3.getRight();

        assertThat(schema3).isEqualTo(Schema.Dict.of(
                "id", Schema.Primitive.BIGINT,
                "new_col", Schema.Coli.of(
                        Schema.Primitive.TEXT,
                        Schema.Primitive.BIGINT,
                        Schema.Array.of(Schema.Primitive.DOUBLE
                        )
                )
        ));
        assertThat(object22).usingRecursiveAssertion().isEqualTo(new HashMap<String, Object>() {{
            put("id", 8);
            put("new_col", PartialValue.of("[1.1, 2.2, 3.3]", List.of(1.1, 2.2, 3.3)));
        }});

        var json22 = new ObjectMapper().writeValueAsString(object22);
        assertThat(json22).isEqualTo("{\"new_col\":\"[1.1, 2.2, 3.3]\",\"id\":8}");
    }
}
