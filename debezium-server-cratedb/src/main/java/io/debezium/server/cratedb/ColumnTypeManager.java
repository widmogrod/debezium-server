/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

public class ColumnTypeManager {
    private final ObjectType schema = new ObjectType();

    public void fromInformationSchema(InformationSchemaColumnInfo[] columns) {
        for (InformationSchemaColumnInfo column : columns) {
            ColumnType columnType = switch (column.dataType()) {
                case "bigint" -> new BigIntType();
                case "text" -> new TextType();
                case "object" -> new ObjectType();
                case "bigint_array" -> new ArrayType(new BigIntType());
                default -> throw new IllegalArgumentException("Unknown data type: " + column.dataType());
            };

            ColumnName columnName = ColumnName.normalized(column.columnName(), columnType);

            InformationSchemaColumnDetails detail = column.columnDetails();
            if (!detail.path().isEmpty()) {
                ColumnType parentType = new ObjectType();
                columnName = ColumnName.normalized(detail.name(), parentType);
                for (String path : detail.path()) {
                    ColumnType currentColumnType = new ObjectType();
                    // if is last then take the column type from the column
                    if (path.equals(detail.path().get(detail.path().size() - 1))) {
                        currentColumnType = columnType;
                    }

                    ColumnName currentColumnName = ColumnName.normalized(path, currentColumnType);
                    ObjectType current = (ObjectType) ObjectType.of(currentColumnName, currentColumnType);
                    parentType.merge(current);
                    parentType = current;
                }
                columnType = parentType;
            }

            schema.mergeColumn(columnName, columnType);
        }
    }

    public ColumnTypeManager addColumn(ColumnName columnName, ColumnType columnType) {
        schema.mergeColumn(columnName, columnType);
        return this;
    }

    public String typeName(ColumnType c) {
        return switch (c) {
            case BigIntType() -> "BIGINT";
            case TextType() -> "TEXT";
            case ArrayType(ColumnType elementType) -> "ARRAY[" + typeName(elementType) + "]";
            case ObjectType ot -> "OBJECT";
        };
    }

    public String typeShape(ColumnType c) {
        return switch (c) {
            case BigIntType() -> typeName(c);
            case TextType() -> typeName(c);
            case ArrayType(ColumnType elementType) ->
                    "ARRAY[" + typeShape(elementType) + "]";
            //CHECKSTYLE:OFF
            case ObjectType ot -> {
                //CHECKSTYLE:ON
                StringBuilder sb = new StringBuilder();
                sb.append("OBJECT");
                if (ot.isEmpty()) {
                    yield sb.toString();
                }

                sb.append(" AS (");
                ot.forEach((k, v) -> {
                    sb.append("\n\t");
                    sb.append(k.columnName()).append(": {\n");
                    v.forEach((k2, v2) -> {
                        sb.append("\t\t");
                        sb.append(typeName(k2));

                        // column info
                        sb.append(": {primaryKey: ");
                        sb.append(v2.primaryKey());
                        sb.append(", columnName: ");
                        sb.append(v2.columnName().columnName());
                        sb.append("}");

                        if (isStruct(k2)) {
                            sb.append(" AS ");
                            sb.append(padLeftNewLines(2, typeShape(k2)));
                        }

                        sb.append(",\n");
                    });
                    sb.append("\t}");
                });
                sb.append("\n)");

                yield sb.toString();
            }
        };
    }

    private boolean isStruct(ColumnType c) {
        return switch (c) {
            case ArrayType(ColumnType elementType) -> isStruct(elementType);
            case ObjectType ot -> !ot.isEmpty();
            default -> false;
        };
    }

    public String padLeftNewLines(int n, String s) {
        // if number of line is 1, no need to pad
        if (s.lines().count() == 1) {
            return s;
        }

        return s.replaceAll("(?m)^", "\t".repeat(n));
    }

    public void print() {
        System.out.println(typeShape(schema));
    }

    public void putCollision(ColumnName name, TypeCollision collision) {
        this.schema.put(name, collision);
    }
}
