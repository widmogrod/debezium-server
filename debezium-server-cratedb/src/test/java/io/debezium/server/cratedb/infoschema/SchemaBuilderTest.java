/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.infoschema;

import io.debezium.server.cratedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

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
                "id", Schema.Primitive.BIGINT
        ));
    }

    @Test
    void testCompactTypeSuffixes() {
        ColumnInfo.Builder builder = new ColumnInfo.Builder();
        ColumnInfo column1 = builder.setColumnName("id").setDataType("integer").build();
        ColumnInfo column2 = builder.setColumnName("id_int").setDataType("boolean").build();
        ColumnInfo column3 = builder.setColumnName("id_bool").setDataType("boolean").build();
        ColumnInfo column4 = builder.setColumnName("id_text").setDataType("text").build();
        ColumnInfo column5 = builder.setColumnName("id_text_bool").setDataType("text").build();
        List<ColumnInfo> columns = List.of(column1, column2, column3, column4, column5);
        Schema.I result = SchemaBuilder.fromInformationSchema(columns);
        assertThat(result).isEqualTo(Schema.Dict.of(
                "id", Schema.Coli.of(
                        Schema.Primitive.BIGINT,
                        Schema.Primitive.BOOLEAN,
                        Schema.Primitive.TEXT
                ),
                "id_text_bool", Schema.Primitive.TEXT
        ));
    }
}