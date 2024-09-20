/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.infoschema;

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
        Boolean isPrimaryKey,
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
        Integer characterMaximumLength
) {

    public boolean isArray() {
        return dataType.endsWith("_array");
    }

    public InformationSchemaColumnInfo subArray() {
        return new InformationSchemaColumnInfo(
                dataType.substring(0, dataType.length() - "_array".length()),
                columnName,
                columnDetails,
                isPrimaryKey,
                characterMaximumLength
        );
    }

    public static class Builder {
        private String dataType;
        private String columnName;
        private InformationSchemaColumnDetails columnDetails;
        private Boolean isPrimaryKey = false;
        private Integer characterMaximumLength = 0;

        public Builder setDataType(String dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder setColumnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder setColumnDetails(InformationSchemaColumnDetails columnDetails) {
            this.columnDetails = columnDetails;
            return this;
        }

        public Builder setIsPrimaryKey(Boolean isPrimaryKey) {
            this.isPrimaryKey = isPrimaryKey;
            return this;
        }

        public Builder setCharacterMaximumLength(Integer characterMaximumLength) {
            this.characterMaximumLength = characterMaximumLength;
            return this;
        }

        public InformationSchemaColumnInfo build() {
            return new InformationSchemaColumnInfo(
                    dataType,
                    columnName,
                    columnDetails,
                    isPrimaryKey,
                    characterMaximumLength
            );
        }
    }
}
