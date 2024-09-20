/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import static io.debezium.server.cratedb.ColumnTypeManager.extractNestedArrayTypes;
import static io.debezium.server.cratedb.ColumnTypeManager.printAlterTable;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.server.cratedb.infoschema.ColumnDetails;
import io.debezium.server.cratedb.infoschema.ColumnInfo;
import io.debezium.server.cratedb.types.ArrayType;
import io.debezium.server.cratedb.types.BigIntType;
import io.debezium.server.cratedb.types.ColumnType;
import io.debezium.server.cratedb.types.ObjectType;
import io.debezium.server.cratedb.types.TextType;

class ColumnTypeTest {
    @Test
    void testComposition() {
        ColumnTypeManager manager = new ColumnTypeManager();
        var info = manager.addColumn(new ColumnName("id"), new BigIntType());
        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("id")));

        info = manager.addColumn(new ColumnName("name"), new TextType());
        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("name")));

        info = manager.addColumn(new ColumnName("name"), new ArrayType(new TextType()));
        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("name_text_array")));

        info = manager.addColumn(new ColumnName("cars"), new ArrayType(new ObjectType()));
        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("cars")));

        info = manager.addColumn(new ColumnName("cars"), new BigIntType());
        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("cars_bigint")));

        info = manager.addColumn(new ColumnName("cars"), new ArrayType(new BigIntType()));
        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("cars_bigint_array")));

        info = manager.addColumn(new ColumnName("cars"), new ArrayType(new ArrayType(new BigIntType())));
        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("cars_bigint_array_array")));

        info = manager.addColumn(new ColumnName("cars"), ObjectType.of(new ColumnName("oid"), new BigIntType()));
        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("cars_object")));

        info = manager.addColumn(new ColumnName("cars"), new ObjectType());
        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("cars_object")));

        info = manager.addColumn(new ColumnName("cars"), ObjectType.of(new ColumnName("did"), new BigIntType()));
        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("cars_object")));

        info = manager.addColumn(new ColumnName("mars"), new ArrayType(new ArrayType(new ArrayType(ObjectType.of(new ColumnName("did"), new BigIntType())))));
        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("mars")));

        info = manager.addColumn(new ColumnName("mars"), new ArrayType(new ArrayType(new ArrayType(ObjectType.of(new ColumnName("did"), new TextType())))));
        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("mars")));

        manager.print();
    }

    @Test
    void testFromObject() {
        Map<String, Object> object1 = Map.of(
                "id", 1,
                "name", "Jon");

        Map<String, Object> object2 = Map.of(
                "id", "asd",
                "name", 2);

        Map<String, Object> object3 = Map.of(
                "id", 3,
                "name", List.of(1, 2, 3));
        Map<String, Object> object4 = Map.of(
                "id", 4,
                "name", Map.of(
                        "pk", 666,
                        "title", "King"));
        Map<String, Object> object5 = Map.of(
                "id", 4,
                "name", Map.of(
                        "pk", List.of(),
                        "title", List.of("King", "Queen")));

        ColumnTypeManager manager = new ColumnTypeManager();
        manager.print();
        Object result1 = manager.fromObject(object1);
        manager.print();
        assertThat(result1).isEqualTo(Map.of(
                "id", 1,
                "name", "Jon"));

        Object result2 = manager.fromObject(object2);
        assertThat(result2).isEqualTo(Map.of(
                "id_text", "asd",
                "name_bigint", 2));

        manager.print();
        Object result3 = manager.fromObject(object3);
        manager.print();
        assertThat(result3).isEqualTo(Map.of(
                "id", 3,
                "name_bigint_array", List.of(1, 2, 3)));

        Object result4 = manager.fromObject(object4);
        assertThat(result4).isEqualTo(Map.of(
                "id", 4,
                "name_object", Map.of(
                        "pk", 666,
                        "title", "King")));

        manager.print();
        Object result5 = manager.fromObject(object5);
        manager.print();

        assertThat(result5).isEqualTo(Map.of(
                "id", 4,
                "name_object", Map.of(
                        "title_text_array", List.of("King", "Queen"))));

        manager.print();
    }

    @Test
    void polyLists() {
        ColumnTypeManager manager = new ColumnTypeManager();
        Object result = manager.fromObject(
                Map.of("doc", Map.of(
                        "poly", List.of(
                                1,
                                false,
                                -2.3,
                                List.of(1, Map.of("id2", 1, "name2", "Jon"), 3),
                                Map.of("id1", 1, "name1", "Jon")))));
        manager.print();

        assertThat(result).isEqualTo(Map.of("doc", Map.of(
                "poly", List.of(1),
                "poly_boolean_array", List.of(false),
                "poly_real_array", List.of(-2.3),
                "poly_bigint_array_array", List.of(1, 3),
                "poly_object_array_array", List.of(Map.of("id2", 1, "name2", "Jon")),
                "poly_object_array", List.of(Map.of("id1", 1, "name1", "Jon")))));

        var nested = extractNestedArrayTypes(manager.getSchema());
        assertThat(nested).isEqualTo(Map.of(
                List.of(new ColumnName("doc"), new ColumnName("poly_bigint_array_array")), new ArrayType(new ArrayType(new BigIntType())),
                List.of(new ColumnName("doc"), new ColumnName("poly_object_array_array")), new ArrayType(new ArrayType(new ObjectType()))));

        var alters = printAlterTable("test", nested);
        assertThat(alters).isEqualTo(List.of(
                "ALTER TABLE test ADD COLUMN doc['poly_object_array_array'] ARRAY(ARRAY(OBJECT))",
                "ALTER TABLE test ADD COLUMN doc['poly_bigint_array_array'] ARRAY(ARRAY(BIGINT))"));
    }

    @Test
    void err1() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ColumnTypeManager manager = new ColumnTypeManager();

        Object o1 = mapper.readValue("""
                {"boolean":666}""", Object.class);
        Object o2 = mapper.readValue("""
                {"boolean":{"truth":false,"lucky":444}}""", Object.class);

        Object r1 = manager.fromObject(o1);
        manager.print();

        Object r2 = manager.fromObject(o2);
        manager.print();
    }

    @Test
    void err2() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ColumnTypeManager manager = new ColumnTypeManager();

        Object o1 = mapper.readValue("""
                {"object":[{"lucky":444,"truth":false},{"lucky":444,"truth":false}],"role":"King",";":-123.31239,"id":4}""", Object.class);

        Object r1 = manager.fromObject(o1);
        manager.print();
    }

    @Test
    void err3() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ColumnTypeManager manager = new ColumnTypeManager();

        Object o1 = mapper.readValue("""
                {"name_boolean":"Queen"}""", Object.class);
        Object o2 = mapper.readValue("""
                {"name_boolean":true}""", Object.class);

        Object r1 = manager.fromObject(o1);
        manager.print();

        assertThat(r1).isEqualTo(Map.of("name_boolean", "Queen"));

        Object r2 = manager.fromObject(o2);
        manager.print();
        assertThat(r2).isEqualTo(Map.of("name_boolean_boolean", true));
    }

    @Test
    void testInformationSchema() {
        ColumnTypeManager manager = new ColumnTypeManager();

        List<ColumnInfo> infos = List.of(
                new ColumnInfo.Builder()
                        .setDataType("object")
                        .setColumnName("doc")
                        .setColumnDetails(new ColumnDetails(
                                "doc",
                                List.of()))
                        .build(),
                new ColumnInfo.Builder()
                        .setDataType("text")
                        .setColumnName("doc['name']")
                        .setColumnDetails(new ColumnDetails(
                                "doc",
                                List.of("name")))
                        .build(),
                new ColumnInfo.Builder()
                        .setDataType("bigint_array")
                        .setColumnName("doc['name_bigint_array']")
                        .setColumnDetails(new ColumnDetails(
                                "doc",
                                List.of("name_bigint_array")))
                        .build(),
                new ColumnInfo.Builder()
                        .setDataType("bigint_array")
                        .setColumnName("arr")
                        .setColumnDetails(new ColumnDetails(
                                "arr",
                                List.of()))
                        .build());

        manager.fromInformationSchema(infos);
        manager.print();
    }

    @Test
    void testInformationSchema2() {
        ColumnTypeManager manager = new ColumnTypeManager();

        List<ColumnInfo> infos = List.of(
                new ColumnInfo.Builder()
                        .setDataType("object")
                        .setColumnName("doc")
                        .setColumnDetails(new ColumnDetails("doc", List.of()))
                        .setIsPrimaryKey(false)
                        .setCharacterMaximumLength(0)
                        .build(),
                // new InformationSchemaColumnInfo.Builder()
                // .setDataType("text")
                // .setColumnName("doc['role']")
                // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("role")))
                // .setIsPrimaryKey(false)
                // .setCharacterMaximumLength(0)
                // .build(),
                // new InformationSchemaColumnInfo.Builder()
                // .setDataType("bigint")
                // .setColumnName("doc['id']")
                // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("id")))
                // .setIsPrimaryKey(false)
                // .setCharacterMaximumLength(0)
                // .build(),
                // new InformationSchemaColumnInfo.Builder()
                // .setDataType("double precision")
                // .setColumnName("doc['+']")
                // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("+")))
                // .setIsPrimaryKey(false)
                // .setCharacterMaximumLength(0)
                // .build(),
                // new InformationSchemaColumnInfo.Builder()
                // .setDataType("boolean")
                // .setColumnName("doc['name_bigint']")
                // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("name_bigint")))
                // .setIsPrimaryKey(false)
                // .setCharacterMaximumLength(0)
                // .build(),
                // new InformationSchemaColumnInfo.Builder()
                // .setDataType("bigint")
                // .setColumnName("doc['name_timestamp with time zone']")
                // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("name_timestamp with time zone")))
                // .setIsPrimaryKey(false)
                // .setCharacterMaximumLength(0)
                // .build(),
                // new InformationSchemaColumnInfo.Builder()
                // .setDataType("double precision")
                // .setColumnName("doc['}']")
                // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("}")))
                // .setIsPrimaryKey(false)
                // .setCharacterMaximumLength(0)
                // .build(),
                // new InformationSchemaColumnInfo.Builder()
                // .setDataType("bigint")
                // .setColumnName("doc['integer']")
                // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("integer")))
                // .setIsPrimaryKey(false)
                // .setCharacterMaximumLength(0)
                // .build(),
                // new InformationSchemaColumnInfo.Builder()
                // .setDataType("double precision")
                // .setColumnName("doc['%']")
                // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("%")))
                // .setIsPrimaryKey(false)
                // .setCharacterMaximumLength(0)
                // .build(),
                // new InformationSchemaColumnInfo.Builder()
                // .setDataType("double precision")
                // .setColumnName("doc['!']")
                // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("!")))
                // .setIsPrimaryKey(false)
                // .setCharacterMaximumLength(0)
                // .build(),
                // new InformationSchemaColumnInfo.Builder()
                // .setDataType("boolean")
                // .setColumnName("doc['bigint']")
                // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("bigint")))
                // .setIsPrimaryKey(false)
                // .setCharacterMaximumLength(0)
                // .build(),
                new ColumnInfo.Builder()
                        .setDataType("bigint")
                        .setColumnName("doc['character']")
                        .setColumnDetails(new ColumnDetails("doc", List.of("character")))
                        .setIsPrimaryKey(false)
                        .setCharacterMaximumLength(0)
                        .build(),
                // new InformationSchemaColumnInfo.Builder()
                // .setDataType("double precision")
                // .setColumnName("doc['\\']")
                // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("\\")))
                // .setIsPrimaryKey(false)
                // .setCharacterMaximumLength(0)
                // .build(),
                // new InformationSchemaColumnInfo.Builder()
                // .setDataType("bigint")
                // .setColumnName("doc['timestamp with time zone']")
                // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("timestamp with time zone")))
                // .setIsPrimaryKey(false)
                // .setCharacterMaximumLength(0)
                // .build(),
                // new InformationSchemaColumnInfo.Builder()
                // .setDataType("double precision")
                // .setColumnName("doc['|']")
                // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("|")))
                // .setIsPrimaryKey(false)
                // .setCharacterMaximumLength(0)
                // .build(),
                // new InformationSchemaColumnInfo.Builder()
                // .setDataType("boolean_array")
                // .setColumnName("doc['character_boolean_array']")
                // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("character_boolean_array")))
                // .setIsPrimaryKey(false)
                // .setCharacterMaximumLength(0)
                // .build(),

                new ColumnInfo.Builder()
                        .setDataType("object_array")
                        .setColumnName("doc['character_object_array']")
                        .setColumnDetails(new ColumnDetails("doc", List.of("character_object_array")))
                        .setIsPrimaryKey(false)
                        .setCharacterMaximumLength(0)
                        .build(),

                new ColumnInfo.Builder()
                        .setDataType("bigint")
                        .setColumnName("doc['character_object_array']['lucky']")
                        .setColumnDetails(new ColumnDetails("doc", List.of("character_object_array", "lucky")))
                        .setIsPrimaryKey(false)
                        .setCharacterMaximumLength(0)
                        .build(),

                new ColumnInfo.Builder()
                        .setDataType("boolean")
                        .setColumnName("doc['character_object_array']['truth']")
                        .setColumnDetails(new ColumnDetails("doc", List.of("character_object_array", "truth")))
                        .setIsPrimaryKey(false)
                        .setCharacterMaximumLength(0)
                        .build()
        // new InformationSchemaColumnInfo.Builder()
        // .setDataType("double precision")
        // .setColumnName("doc['>']")
        // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of(">")))
        // .setIsPrimaryKey(false)
        // .setCharacterMaximumLength(0)
        // .build(),
        // new InformationSchemaColumnInfo.Builder()
        // .setDataType("text_array")
        // .setColumnName("doc['character_text_array']")
        // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("character_text_array")))
        // .setIsPrimaryKey(false)
        // .setCharacterMaximumLength(0)
        // .build(),
        // new InformationSchemaColumnInfo.Builder()
        // .setDataType("boolean")
        // .setColumnName("doc['double precision']")
        // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("double precision")))
        // .setIsPrimaryKey(false)
        // .setCharacterMaximumLength(0)
        // .build(),
        // new InformationSchemaColumnInfo.Builder()
        // .setDataType("double precision")
        // .setColumnName("doc['-']")
        // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("-")))
        // .setIsPrimaryKey(false)
        // .setCharacterMaximumLength(0)
        // .build(),
        // new InformationSchemaColumnInfo.Builder()
        // .setDataType("text")
        // .setColumnName("doc['character_text']")
        // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("character_text")))
        // .setIsPrimaryKey(false)
        // .setCharacterMaximumLength(0)
        // .build(),
        // new InformationSchemaColumnInfo.Builder()
        // .setDataType("bigint")
        // .setColumnName("doc['name_@']")
        // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("name_@")))
        // .setIsPrimaryKey(false)
        // .setCharacterMaximumLength(0)
        // .build(),
        // new InformationSchemaColumnInfo.Builder()
        // .setDataType("double precision")
        // .setColumnName("doc['']")
        // .setColumnDetails(new InformationSchemaColumnDetails("doc", List.of("")))
        // .setIsPrimaryKey(false)
        // .setCharacterMaximumLength(0)
        // .build()

        );

        manager.fromInformationSchema(infos);
        manager.print();
    }

    @Test
    void testCollisions() {
        TypeCollision collision = new TypeCollision();
        var info = collision.putType(
                new ObjectType(),
                new io.debezium.server.cratedb.ColumnInfo(true, new ColumnName("name")));

        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(true, new ColumnName("name")));

        info = collision.putType(
                new ObjectType(),
                new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("name")));
        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(true, new ColumnName("name")));

        info = collision.putType(
                ObjectType.of(new ColumnName("did"), new BigIntType()),
                new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("name")));
        assertThat(info).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(true, new ColumnName("name")));

        ColumnTypeManager manager = new ColumnTypeManager();
        manager.putCollision(new ColumnName("name"), collision);
        manager.print();
    }

    @Test
    void testCompare1() {
        ColumnType o1 = new ObjectType();
        ColumnType o2 = new ObjectType();

        assertThat(o1).isEqualTo(o2);
        assertThat(o2).isEqualTo(o1);
    }

    @Test
    void testCompare2() {
        ColumnType o1 = new ObjectType();
        ColumnType o2 = ObjectType.of(new ColumnName("did"), new BigIntType());

        assertThat(o1).isEqualTo(o2);
        assertThat(o2).isEqualTo(o1);

        o1.merge(o2);
    }

    @Test
    void testObjectPutAndTry() {
        ObjectType objectType = new ObjectType();
        io.debezium.server.cratedb.ColumnInfo columnInfo = objectType.putColumnNameWithType(new ColumnName("name"), new BigIntType());
        assertThat(columnInfo).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("name")));

        io.debezium.server.cratedb.ColumnInfo columnInfo2 = objectType.putColumnNameWithType(new ColumnName("name"), new ArrayType(new BigIntType()));
        assertThat(columnInfo2).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("name_bigint_array")));

        Optional<Pair<ColumnType, io.debezium.server.cratedb.ColumnInfo>> pair = objectType.tryGetColumnInfoOf(new ColumnName("name"), new BigIntType());
        assertThat(pair).isPresent();
        assertThat(pair.get().getRight()).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("name")));
        assertThat(pair.get().getLeft()).isEqualTo(new BigIntType());

        Optional<Pair<ColumnType, io.debezium.server.cratedb.ColumnInfo>> pair2 = objectType.tryGetColumnInfoOf(new ColumnName("name"), new ArrayType(new BigIntType()));
        assertThat(pair2).isPresent();
        assertThat(pair2.get().getRight()).isEqualTo(new io.debezium.server.cratedb.ColumnInfo(false, new ColumnName("name_bigint_array")));
        assertThat(pair2.get().getLeft()).isEqualTo(new ArrayType(new BigIntType()));
    }
}
