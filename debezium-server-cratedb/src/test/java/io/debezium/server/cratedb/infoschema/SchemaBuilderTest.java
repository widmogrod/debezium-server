/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.infoschema;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.server.cratedb.schema.Schema;

class SchemaBuilderTest {
    @Test
    void testFromInformationSchemaWithEmptyColumnsList() {
        List<ColumnInfo> columns = new ArrayList<>();
        Schema.I result = SchemaBuilder.fromInformationSchema(columns);
        assertThat(result).isEqualTo(Schema.Dict.of());
    }

    @Test
    void testFromInformationSchemaWithValidColumnsList() {
        ColumnInfo.Builder builder = new ColumnInfo.Builder();
        ColumnInfo columnInfo = builder.setColumnName("id").setDataType("integer").build();
        List<ColumnInfo> columns = List.of(columnInfo);
        Schema.I result = SchemaBuilder.fromInformationSchema(columns);
        assertThat(result).isEqualTo(Schema.Dict.of(
                "id", Schema.Primitive.BIGINT));
    }

    @Test
    void testTwoDifferentFieldNoCompactionShouldOccur() {
        ColumnInfo.Builder builder = new ColumnInfo.Builder();
        ColumnInfo column1 = builder.setColumnName("id").setDataType("integer").build();
        ColumnInfo column2 = builder.setColumnName("id_int").setDataType("integer").build();
        ColumnInfo column3 = builder.setColumnName("id_text_bool").setDataType("text").build();
        List<ColumnInfo> columns = List.of(column1, column2, column3);
        Schema.I result = SchemaBuilder.fromInformationSchema(columns);
        assertThat(result).isEqualTo(Schema.Dict.of(
                "id", Schema.Primitive.BIGINT,
                "id_int", Schema.Primitive.BIGINT,
                "id_text_bool", Schema.Primitive.TEXT));
    }

    @Test
    void testTwoDifferentFieldNoCompactionShouldOccur2() {
        ColumnInfo.Builder builder = new ColumnInfo.Builder();
        ColumnInfo column1 = builder.setColumnName("doc['id']").setColumnDetails(new ColumnDetails("doc", List.of("id"))).setDataType("object").build();
        ColumnInfo column2 = builder.setColumnName("doc['id_object']").setColumnDetails(new ColumnDetails("doc", List.of("id_object"))).setDataType("bigint").build();
        ColumnInfo column3 = builder.setColumnName("doc['id_object_text']").setColumnDetails(new ColumnDetails("doc", List.of("id_object_text"))).setDataType("text")
                .build();
        List<ColumnInfo> columns = List.of(column1, column2, column3);
        Schema.I result = SchemaBuilder.fromInformationSchema(columns);
        assertThat(result).isEqualTo(Schema.Dict.of(
                "doc", Schema.Dict.of(
                        "id", Schema.Dict.of(),
                        "id_object", Schema.Coli.of(
                                Schema.Primitive.BIGINT,
                                Schema.Primitive.TEXT))));
    }

    @Test
    void testTwoDifferentFieldNoCompactionShouldOccurInNested() {
        ColumnInfo.Builder builder = new ColumnInfo.Builder();
        ColumnInfo column1 = builder.setColumnName("doc['other']['id']").setColumnDetails(new ColumnDetails("doc", List.of("other", "id"))).setDataType("integer")
                .build();
        ColumnInfo column2 = builder.setColumnName("doc['other']['id_int']").setColumnDetails(new ColumnDetails("doc", List.of("other", "id_int"))).setDataType("integer")
                .build();
        ColumnInfo column3 = builder.setColumnName("doc['other']['id_text_bool']").setColumnDetails(new ColumnDetails("doc", List.of("other", "id_text_bool")))
                .setDataType("text").build();

        List<ColumnInfo> columns = List.of(column1, column2, column3);
        Schema.I result = SchemaBuilder.fromInformationSchema(columns);
        assertThat(result).isEqualTo(Schema.Dict.of(
                "doc", Schema.Dict.of(
                        "other", Schema.Dict.of(
                                "id", Schema.Primitive.BIGINT,
                                "id_int", Schema.Primitive.BIGINT,
                                "id_text_bool", Schema.Primitive.TEXT))));
    }

    @Test
    void testCompactTypeSuffixes() {
        ColumnInfo.Builder builder = new ColumnInfo.Builder();
        ColumnInfo column1 = builder.setColumnName("id").setDataType("integer").build();
        ColumnInfo column2 = builder.setColumnName("id_int").setDataType("integer").build();
        ColumnInfo column3 = builder.setColumnName("id_bool").setDataType("boolean").build();
        ColumnInfo column4 = builder.setColumnName("id_text").setDataType("text").build();
        ColumnInfo column5 = builder.setColumnName("id_text_bool").setDataType("text").build();
        List<ColumnInfo> columns = List.of(column1, column2, column3, column4, column5);
        Schema.I result = SchemaBuilder.fromInformationSchema(columns);
        assertThat(result).isEqualTo(Schema.Dict.of(
                "id", Schema.Coli.of(
                        Schema.Primitive.BIGINT,
                        Schema.Primitive.BOOLEAN,
                        Schema.Primitive.TEXT),
                "id_int", Schema.Primitive.BIGINT,
                "id_text_bool", Schema.Primitive.TEXT));
    }

    @Test
    void testCompactTypeSuffixesInNestedDocs() {
        ColumnInfo.Builder builder = new ColumnInfo.Builder();
        ColumnInfo column1 = builder.setColumnName("doc['id']").setColumnDetails(new ColumnDetails("doc", List.of("id"))).setDataType("integer").build();
        ColumnInfo column2 = builder.setColumnName("doc['id_int']").setColumnDetails(new ColumnDetails("doc", List.of("id_int"))).setDataType("integer").build();
        ColumnInfo column3 = builder.setColumnName("doc['id_bool']").setColumnDetails(new ColumnDetails("doc", List.of("id_bool"))).setDataType("boolean").build();
        ColumnInfo column4 = builder.setColumnName("doc['id_text']").setColumnDetails(new ColumnDetails("doc", List.of("id_text"))).setDataType("text").build();
        ColumnInfo column5 = builder.setColumnName("doc['id_text_bool']").setColumnDetails(new ColumnDetails("doc", List.of("id_text_bool"))).setDataType("text").build();
        List<ColumnInfo> columns = List.of(column1, column2, column3, column4, column5);
        Schema.I result = SchemaBuilder.fromInformationSchema(columns);
        assertThat(result).isEqualTo(Schema.Dict.of(
                "doc", Schema.Dict.of(
                        "id", Schema.Coli.of(
                                Schema.Primitive.BIGINT,
                                Schema.Primitive.BOOLEAN,
                                Schema.Primitive.TEXT),
                        "id_int", Schema.Primitive.BIGINT,
                        "id_text_bool", Schema.Primitive.TEXT)));
    }

    @Test
    void testCompactTypeSuffixesInNestedDocs2() {
        ColumnInfo.Builder builder = new ColumnInfo.Builder();
        ColumnInfo column1 = builder.setColumnName("doc['other']['id']").setColumnDetails(new ColumnDetails("doc", List.of("other", "id"))).setDataType("integer")
                .build();
        ColumnInfo column2 = builder.setColumnName("doc['other']['id_int']").setColumnDetails(new ColumnDetails("doc", List.of("other", "id_int"))).setDataType("integer")
                .build();
        ColumnInfo column3 = builder.setColumnName("doc['other']['id_bool']").setColumnDetails(new ColumnDetails("doc", List.of("other", "id_bool")))
                .setDataType("boolean").build();
        ColumnInfo column4 = builder.setColumnName("doc['other']['id_text']").setColumnDetails(new ColumnDetails("doc", List.of("other", "id_text"))).setDataType("text")
                .build();
        ColumnInfo column5 = builder.setColumnName("doc['other']['id_text_bool']").setColumnDetails(new ColumnDetails("doc", List.of("other", "id_text_bool")))
                .setDataType("text").build();
        List<ColumnInfo> columns = List.of(column1, column2, column3, column4, column5);
        Schema.I result = SchemaBuilder.fromInformationSchema(columns);
        assertThat(result).isEqualTo(Schema.Dict.of(
                "doc", Schema.Dict.of(
                        "other", Schema.Dict.of(
                                "id", Schema.Coli.of(
                                        Schema.Primitive.BIGINT,
                                        Schema.Primitive.BOOLEAN,
                                        Schema.Primitive.TEXT),
                                "id_int", Schema.Primitive.BIGINT,
                                "id_text_bool", Schema.Primitive.TEXT))));
    }

    @Test
    void testNameCollision() {
        List<ColumnInfo> columns = new ArrayList<>();

        ColumnInfo column1 = new ColumnInfo.Builder()
                .setDataType("text")
                .setColumnName("id")
                .setIsPrimaryKey(true)
                .setColumnDetails(new ColumnDetails("id", new ArrayList<>()))
                .setCharacterMaximumLength(0)
                .build();

        ColumnInfo column2 = new ColumnInfo.Builder()
                .setDataType("object")
                .setColumnName("doc")
                .setIsPrimaryKey(false)
                .setColumnDetails(new ColumnDetails("doc", new ArrayList<>()))
                .setCharacterMaximumLength(0)
                .build();

        ColumnInfo column3 = new ColumnInfo.Builder()
                .setDataType("object")
                .setColumnName("doc['name']")
                .setIsPrimaryKey(false)
                .setColumnDetails(new ColumnDetails("doc", new ArrayList<String>() {
                    {
                        add("name");
                    }
                }))
                .setCharacterMaximumLength(0)
                .build();

        ColumnInfo column4 = new ColumnInfo.Builder()
                .setDataType("bigint")
                .setColumnName("doc['name']['lucky']")
                .setIsPrimaryKey(false)
                .setColumnDetails(new ColumnDetails("doc", new ArrayList<String>() {
                    {
                        add("name");
                        add("lucky");
                    }
                }))
                .setCharacterMaximumLength(0)
                .build();

        ColumnInfo column5 = new ColumnInfo.Builder()
                .setDataType("boolean")
                .setColumnName("doc['name']['truth']")
                .setIsPrimaryKey(false)
                .setColumnDetails(new ColumnDetails("doc", new ArrayList<String>() {
                    {
                        add("name");
                        add("truth");
                    }
                }))
                .setCharacterMaximumLength(0)
                .build();

        ColumnInfo column10 = new ColumnInfo.Builder()
                .setDataType("bigint")
                .setColumnName("doc['name_object']")
                .setIsPrimaryKey(false)
                .setColumnDetails(new ColumnDetails("doc", new ArrayList<String>() {
                    {
                        add("name_object");
                    }
                }))
                .setCharacterMaximumLength(0)
                .build();

        ColumnInfo column19 = new ColumnInfo.Builder()
                .setDataType("text")
                .setColumnName("doc['name_object_text']")
                .setIsPrimaryKey(false)
                .setColumnDetails(new ColumnDetails("doc", new ArrayList<String>() {
                    {
                        add("name_object_text");
                    }
                }))
                .setCharacterMaximumLength(0)
                .build();

        ColumnInfo column22 = new ColumnInfo.Builder()
                .setDataType("bigint")
                .setColumnName("doc['name_int']")
                .setIsPrimaryKey(false)
                .setColumnDetails(new ColumnDetails("doc", new ArrayList<String>() {
                    {
                        add("name_int");
                    }
                }))
                .setCharacterMaximumLength(0)
                .build();

        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        columns.add(column4);
        columns.add(column5);
        columns.add(column10);
        columns.add(column19);
        columns.add(column22);

        Schema.I expected = Schema.Dict.of(
                "name_object", Schema.Coli.of(
                        Schema.Primitive.BIGINT,
                        Schema.Primitive.TEXT),
                "name", Schema.Coli.of(
                        Schema.Dict.of(
                                "lucky", Schema.Primitive.BIGINT,
                                "truth", Schema.Primitive.BOOLEAN),
                        Schema.Primitive.BIGINT));

        Schema.I result = SchemaBuilder.fromInformationSchema(columns);
        assertThat(result).isEqualTo(Schema.Dict.of(
                "id", Schema.Primitive.TEXT,
                "doc", expected));
    }

    @Test
    void useCaseArrayObjectReconstruction() {
        List<ColumnInfo> columns = new ArrayList<>();
        ColumnInfo column1 = new ColumnInfo.Builder()
                .setDataType("object_array")
                .setColumnName("doc['name_{']")
                .setIsPrimaryKey(false)
                .setColumnDetails(new ColumnDetails("doc", List.of("name_{")))
                .setCharacterMaximumLength(0)
                .build();
        columns.add(column1);

        ColumnInfo column3 = new ColumnInfo.Builder()
                .setDataType("bigint")
                .setColumnName("doc['name_{']['lucky']")
                .setIsPrimaryKey(false)
                .setColumnDetails(new ColumnDetails("doc", List.of("name_{", "lucky")))
                .setCharacterMaximumLength(0)
                .build();
        columns.add(column3);

        ColumnInfo column4 = new ColumnInfo.Builder()
                .setDataType("boolean")
                .setColumnName("doc['name_{']['truth']")
                .setIsPrimaryKey(false)
                .setColumnDetails(new ColumnDetails("doc", List.of("name_{", "truth")))
                .setCharacterMaximumLength(0)
                .build();
        columns.add(column4);

        Schema.I result = SchemaBuilder.fromInformationSchema(columns);
        assertThat(result).isEqualTo(Schema.Dict.of(
                "doc", Schema.Dict.of(
                        "name_{",
                        Schema.Array.of(Schema.Dict.of(
                                "lucky", Schema.Primitive.BIGINT,
                                "truth", Schema.Primitive.BOOLEAN)))));
    }
}
