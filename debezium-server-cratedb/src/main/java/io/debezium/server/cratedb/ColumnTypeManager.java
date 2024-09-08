/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ColumnTypeManager {
    private static final DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ofPattern("[MM/dd/yyyy]" + "[dd-MM-yyyy]" + "[yyyy-MM-dd]")).append(DateTimeFormatter.ofPattern("[HH:mm:ss]")).toFormatter();
    private static final String NUMBER_REGEX = "[+-]?(\\d+([.]\\d*)?(e[+-]?\\d+)?|[.]\\d+(e[+-]?\\d+)?)";
    private static final String WHITESPACES_REGEX = "\\s*";
    private static final String POINT_REGEX = "POINT";
    private static final Pattern pointPattern = Pattern.compile(POINT_REGEX + WHITESPACES_REGEX + "\\(" + NUMBER_REGEX + WHITESPACES_REGEX + NUMBER_REGEX + "\\)");

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

    public Object fromObject(Object o) {
        return fromObject(o, schema);
    }

    public Object fromObject(Object o, ObjectType schema) {
        // traverse object and update schema
        // and use schema information to update object spec

        return switch (o) {
            case String x -> {
                if (x.isEmpty()) {
                    yield null;
                }

                ObjectMapper mapper = new ObjectMapper();
                try {
                    Object json = mapper.readValue(x, Object.class);
                    yield fromObject(json);
                }
                catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    OffsetDateTime time = OffsetDateTime.parse(x);
                    yield time;
                }
                catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    ZonedDateTime time = ZonedDateTime.parse(x);
                    yield time;
                }
                catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    LocalTime time = LocalTime.parse(x);
                    yield time;
                }
                catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    LocalDateTime time = LocalDateTime.parse(x);
                    yield time;
                }
                catch (Exception ignored) {
                    // DO nothing
                }

                yield x;
            }
            case Integer x -> x;
            case List x -> {
                if (x.isEmpty()) {
                    yield null;
                }

                List<Object> result = new ArrayList<>();
                for (Object value : x) {
                    result.add(fromObject(value, schema));
                }

                yield result;
            }

            case Map x -> {
                if (x.isEmpty()) {
                    yield null;
                }

                Map<Object, Object> result = new LinkedHashMap<>();
                for (Object key : x.keySet()) {
                    Object value = x.get(key);
                    // if is empty array, skip
                    if (value instanceof List list && list.isEmpty()) {
                        continue;
                    }

                    ObjectType columnType = new ObjectType();
                    ColumnName columnName = ColumnName.normalized(key.toString(), columnType);
                    var info = schema.getObjectType(columnName);
                    if (info.isPresent()) {
                        columnType = info.get().getLeft();
                        columnName = info.get().getRight().columnName();
                    } else {
                        var info2 = schema.mergeColumn(columnName, detect(value));
                        columnName = info2.columnName();
                    }
                    result.put(columnName.columnName(), fromObject(value, columnType));
                }

                yield result;
            }
            default -> throw new IllegalArgumentException("Unknown object type: " + o.getClass());
        };
    }

    public ColumnType detect(Object o) {
        return switch (o) {
            case String x -> {
                if (x.isEmpty()) {
                    yield new TextType();
                }

                ObjectMapper mapper = new ObjectMapper();
                try {
                    Object json = mapper.readValue(x, Object.class);
                    yield detect(json);
                }
                catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    OffsetDateTime time = OffsetDateTime.parse(x);
                    yield detect(time);
                }
                catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    ZonedDateTime time = ZonedDateTime.parse(x);
                    yield detect(time);
                }
                catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    LocalTime time = LocalTime.parse(x);
                    yield detect(time);
                }
                catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    LocalDateTime time = LocalDateTime.parse(x);
                    yield detect(time);
                }
                catch (Exception ignored) {
                    // DO nothing
                }

//                Matcher pointMatcher = pointPattern.matcher(x);
//                if (pointMatcher.matches()) {
//                    yield new CrateDBType(CrateDBType.Type.GEO_POINT);
//                }

                yield new TextType();
            }

            case Integer x -> new BigIntType();
            case Long x -> new BigIntType();
//            case Float x -> new CrateDBType(CrateDBType.Type.REAL);
//            case Double x -> new CrateDBType(CrateDBType.Type.REAL);
//            case Boolean x -> new CrateDBType(CrateDBType.Type.BOOLEAN);
//            case java.sql.Timestamp x -> new CrateDBType(CrateDBType.Type.TIMESTAMP);
//            case java.time.Instant x -> new CrateDBType(CrateDBType.Type.TIMESTAMP);
//            case java.time.OffsetDateTime x -> new CrateDBType(CrateDBType.Type.TIMESTAMPTZ);
//            case java.time.ZonedDateTime x -> new CrateDBType(CrateDBType.Type.TIMESTAMPTZ);
//            case java.time.LocalDateTime x -> new CrateDBType(CrateDBType.Type.TIMESTAMPWOZ);
//            case java.time.LocalDate x -> new CrateDBType(CrateDBType.Type.DATE);
//            case java.time.LocalTime x -> new CrateDBType(CrateDBType.Type.TIME);
//            case java.net.InetAddress x -> new CrateDBType(CrateDBType.Type.IP);

            case List x -> {
                if (x.isEmpty()) {
                    throw new RuntimeException("Empty array");
                }

                yield new ArrayType(detect(x.get(0)));
            }

            case Map x -> {
                if (x.isEmpty()) {
                    yield new ObjectType();
                }

                ColumnType result = new ObjectType();
                for (Object key : x.keySet()) {
                    ColumnName columnName = ColumnName.normalized(key.toString(), new ObjectType());
                    // if is empty array, skip
                    if (x.get(key) instanceof List list && list.isEmpty()) {
                        continue;
                    }

                    ColumnType columnType = detect(x.get(key));
                    ObjectType current = (ObjectType) ObjectType.of(columnName, columnType);
                    result.merge(current);
                }

                yield result;
            }

            default -> throw new IllegalStateException("Unexpected value: " + o);
        };
    }

    public ColumnInfo addColumn(ColumnName columnName, ColumnType columnType) {
        return schema.mergeColumn(columnName, columnType);
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
            case ObjectType ot -> {
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
