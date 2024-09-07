/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

public record InformationSchemaColumnInfo(
//        String udtSchema,
//        String udtName,
//        String udtCatalog,
//        String tableSchema,
//        String tableName,
//        String tableCatalog,
//        Integer ordinalPosition,
//        Integer numericScale,
//        Integer numericPrecisionRadix,
//        Integer numericPrecision,
//        Boolean isNullable,
//        Boolean isIdentity,
//        String isGenerated,
//        String intervalType,
//        Integer intervalPrecision,
//        String identityStart,
//        String identityMinimum,
//        String identityMaximum,
//        String identityIncrement,
//        String identityGeneration,
//        Boolean identityCycle,
//        String generationExpression,
//        String domainSchema,
//        String domainName,
//        String domainCatalog,
//        Integer datetimePrecision,
        String dataType,
        String columnName,
        InformationSchemaColumnDetails columnDetails,
        Boolean isPrimaryKey
//        String columnDefault,
//        String collationSchema,
//        String collationName,
//        String collationCatalog,
//        String checkReferences,
//        Integer checkAction,
//        String characterSetSchema,
//        String characterSetName,
//        String characterSetCatalog,
//        Integer characterOctetLength,
//        Integer characterMaximumLength
) {
}
