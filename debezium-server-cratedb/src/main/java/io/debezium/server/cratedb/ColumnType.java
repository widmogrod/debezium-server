/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

public sealed interface ColumnType permits ArrayType, BigIntType, BitType, BooleanType, CharType, FloatType, GeoShapeType, ObjectType, TextType, TimezType {

    String shortName();

    void merge(ColumnType columnType);
}
