/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

public sealed interface ColumnType permits BigIntType, TextType, ArrayType, ObjectType {

    String shortName();

    void merge(ColumnType columnType);
}
