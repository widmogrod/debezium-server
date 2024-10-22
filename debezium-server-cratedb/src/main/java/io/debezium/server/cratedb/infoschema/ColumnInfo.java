/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.infoschema;

/**
 * Represents information about a column in a database table.
 */
public record ColumnInfo(
        String dataType,
        String columnName,
        ColumnDetails columnDetails,
        Boolean isPrimaryKey,
        Integer characterMaximumLength
) {

    public boolean isArray() {
        return dataType.endsWith("_array");
    }

    public ColumnInfo subArray() {
        return new ColumnInfo(
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
        private ColumnDetails columnDetails;
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

        public Builder setColumnDetails(ColumnDetails columnDetails) {
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

        public ColumnInfo build() {
            return new ColumnInfo(
                    dataType,
                    columnName,
                    columnDetails,
                    isPrimaryKey,
                    characterMaximumLength
            );
        }
    }
}
