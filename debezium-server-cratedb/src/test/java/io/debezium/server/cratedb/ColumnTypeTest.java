/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class ColumnTypeTest {
    @Test
    void testComposition() {
        ColumnTypeManager manager = new ColumnTypeManager();
        var info = manager.addColumn(new ColumnName("id"), new BigIntType());
        assertThat(info).isEqualTo(new ColumnInfo(false, new ColumnName("id")));

        info = manager.addColumn(new ColumnName("name"), new TextType());
        assertThat(info).isEqualTo(new ColumnInfo(false, new ColumnName("name")));

        info = manager.addColumn(new ColumnName("name"), new ArrayType(new TextType()));
        assertThat(info).isEqualTo(new ColumnInfo(false, new ColumnName("name_text_array")));

        info = manager.addColumn(new ColumnName("cars"), new ArrayType(new ObjectType()));
        assertThat(info).isEqualTo(new ColumnInfo(false, new ColumnName("cars")));

        info = manager.addColumn(new ColumnName("cars"), new BigIntType());
        assertThat(info).isEqualTo(new ColumnInfo(false, new ColumnName("cars_bigint")));

        info = manager.addColumn(new ColumnName("cars"), new ArrayType(new BigIntType()));
        assertThat(info).isEqualTo(new ColumnInfo(false, new ColumnName("cars_bigint_array")));

        info = manager.addColumn(new ColumnName("cars"), new ArrayType(new ArrayType(new BigIntType())));
        assertThat(info).isEqualTo(new ColumnInfo(false, new ColumnName("cars_bigint_array_array")));

        info = manager.addColumn(new ColumnName("cars"), ObjectType.of(new ColumnName("oid"), new BigIntType()));
        assertThat(info).isEqualTo(new ColumnInfo(false, new ColumnName("cars_object")));

        info = manager.addColumn(new ColumnName("cars"), new ObjectType());
        assertThat(info).isEqualTo(new ColumnInfo(false, new ColumnName("cars_object")));

        info = manager.addColumn(new ColumnName("cars"), ObjectType.of(new ColumnName("did"), new BigIntType()));
        assertThat(info).isEqualTo(new ColumnInfo(false, new ColumnName("cars_object")));

        info = manager.addColumn(new ColumnName("mars"), new ArrayType(new ArrayType(new ArrayType(ObjectType.of(new ColumnName("did"), new BigIntType())))));
        assertThat(info).isEqualTo(new ColumnInfo(false, new ColumnName("mars")));

        info = manager.addColumn(new ColumnName("mars"), new ArrayType(new ArrayType(new ArrayType(ObjectType.of(new ColumnName("did"), new TextType())))));
        assertThat(info).isEqualTo(new ColumnInfo(false, new ColumnName("mars")));

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
        Object result1 = manager.fromObject(object1);
        assertThat(result1).isEqualTo(Map.of(
                "id", 1,
                "name", "Jon"));

        Object result2 = manager.fromObject(object2);
        assertThat(result2).isEqualTo(Map.of(
                "id_text", "asd",
                "name_bigint", 2));

        Object result3 = manager.fromObject(object3);
        assertThat(result3).isEqualTo(Map.of(
                "id", 3,
                "name_bigint_array", List.of(1, 2, 3)));

        Object result4 = manager.fromObject(object4);
        assertThat(result4).isEqualTo(Map.of(
                "id", 4,
                "name_object", Map.of(
                        "pk", 666,
                        "title", "King")));

        Object result5 = manager.fromObject(object5);
        assertThat(result5).isEqualTo(Map.of(
                "id", 4,
                "name_object", Map.of(
                        "title_text_array", List.of("King", "Queen"))));

        manager.print();
    }

    @Test
    void testInformationSchema() {
        ColumnTypeManager manager = new ColumnTypeManager();

        InformationSchemaColumnInfo[] infos = new InformationSchemaColumnInfo[]{
                new InformationSchemaColumnInfo(
                        "text",
                        "id",
                        new InformationSchemaColumnDetails(
                                "id",
                                List.of()),
                        true),
                new InformationSchemaColumnInfo(
                        "object",
                        "doc",
                        new InformationSchemaColumnDetails(
                                "doc",
                                List.of()),
                        false),
                new InformationSchemaColumnInfo(
                        "text",
                        "doc['name']",
                        new InformationSchemaColumnDetails(
                                "doc",
                                List.of("name")),
                        false),
                new InformationSchemaColumnInfo(
                        "bigint_array",
                        "doc['name_bigint_array']",
                        new InformationSchemaColumnDetails(
                                "doc",
                                List.of("name_bigint_array")),
                        false),
                new InformationSchemaColumnInfo(
                        "bigint_array",
                        "arr",
                        new InformationSchemaColumnDetails(
                                "arr",
                                List.of()),
                        false),
        };

        manager.fromInformationSchema(infos);
        manager.print();
    }

    @Test
    void testCollisions() {
        TypeCollision collision = new TypeCollision();
        var info = collision.putType(
                new ObjectType(),
                new ColumnInfo(true, new ColumnName("name")));

        assertThat(info).isEqualTo(new ColumnInfo(true, new ColumnName("name")));

        info = collision.putType(
                new ObjectType(),
                new ColumnInfo(false, new ColumnName("name")));
        assertThat(info).isEqualTo(new ColumnInfo(true, new ColumnName("name")));

        info = collision.putType(
                ObjectType.of(new ColumnName("did"), new BigIntType()),
                new ColumnInfo(false, new ColumnName("name")));
        assertThat(info).isEqualTo(new ColumnInfo(true, new ColumnName("name")));

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
}
