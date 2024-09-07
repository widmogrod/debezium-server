/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

class ColumnTypeTest {
    @Test
    void testComposition() {
        ColumnTypeManager manager = new ColumnTypeManager();
        manager
                .addColumn(new ColumnName("id"), new BigIntType())
                .addColumn(new ColumnName("name"), new TextType())
                .addColumn(new ColumnName("name"), new ArrayType(new TextType()))
                .addColumn(new ColumnName("cars"), new ArrayType(new ObjectType()))
                .addColumn(new ColumnName("cars"), new BigIntType())
                .addColumn(new ColumnName("cars"), new ArrayType(new BigIntType()))
                .addColumn(new ColumnName("cars"), new ArrayType(new ArrayType(new BigIntType())))
                .addColumn(new ColumnName("cars"), ObjectType.of(new ColumnName("oid"), new BigIntType()))
                .addColumn(new ColumnName("cars"), new ObjectType())
                .addColumn(new ColumnName("cars"), ObjectType.of(new ColumnName("did"), new BigIntType()))
                .addColumn(new ColumnName("mars"), new ArrayType(new ArrayType(new ArrayType(ObjectType.of(new ColumnName("did"), new BigIntType())))))
                .addColumn(new ColumnName("mars"), new ArrayType(new ArrayType(new ArrayType(ObjectType.of(new ColumnName("did"), new TextType())))));
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
        collision.putType(
                new ObjectType(),
                new ColumnInfo(true, new ColumnName("name")));
        collision.putType(
                new ObjectType(),
                new ColumnInfo(false, new ColumnName("name")));
        collision.putType(
                ObjectType.of(new ColumnName("did"), new BigIntType()),
                new ColumnInfo(false, new ColumnName("name")));

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
