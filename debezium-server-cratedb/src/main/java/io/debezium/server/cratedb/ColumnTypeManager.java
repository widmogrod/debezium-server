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
import java.util.*;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ColumnTypeManager {
    private static final DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ofPattern("[MM/dd/yyyy]" + "[dd-MM-yyyy]" + "[yyyy-MM-dd]")).append(DateTimeFormatter.ofPattern("[HH:mm:ss]")).toFormatter();
    private static final String NUMBER_REGEX = "[+-]?(\\d+([.]\\d*)?(e[+-]?\\d+)?|[.]\\d+(e[+-]?\\d+)?)";
    private static final String WHITESPACES_REGEX = "\\s*";
    private static final String POINT_REGEX = "POINT";
//    private static final Pattern pointPattern = Pattern.compile(POINT_REGEX + WHITESPACES_REGEX + "\\(" + NUMBER_REGEX + WHITESPACES_REGEX + NUMBER_REGEX + "\\)");

    private final ObjectType schema = new ObjectType();

    public static Map<List<ColumnName>, ArrayType> extractNestedArrayTypes(ObjectType schema) {
        return extractNestedArrayTypes(new ArrayList<>(), schema);
    }

    public static Map<List<ColumnName>, ArrayType> extractNestedArrayTypes(List<ColumnName> path, ColumnType schema) {
        return switch (schema) {
            case ObjectType ot -> {
                Map<List<ColumnName>, ArrayType> result = new HashMap<>();
                for (var entry : ot.entrySet()) {
                    for (var type : entry.getValue().entrySet()) {
                        var newPath = new ArrayList<>(path);
                        newPath.add(type.getValue().columnName());

                        var nested = extractNestedArrayTypes(newPath, type.getKey());
                        result.putAll(nested);
                    }
                }
                yield result;
            }
            case ArrayType at -> switch (at.elementType()) {
                case ArrayType at2 -> {
                    var nested = extractNestedArrayTypes(path, at2);
                    nested.putAll(Map.of(path, at));
                    yield nested;
                }
                default -> new HashMap<>();
            };
            default -> new HashMap<>();
        };
    }

    public void fromInformationSchema(List<InformationSchemaColumnInfo> columns) {
        for (InformationSchemaColumnInfo column : columns) {
            ColumnType columnType = getColumnType(column);

            InformationSchemaColumnDetails detail = column.columnDetails();
            ColumnName columnName = ColumnName.normalized(detail.name(), columnType);

            if (!detail.path().isEmpty()) {
                ObjectType rootType = new ObjectType();
                var info = schema.tryGetColumnNamed(columnName);
                if (!info.isEmpty()) {
                    if (info.get().getLeft() instanceof ObjectType ot) {
                        rootType = ot;
                    }
                }

                ObjectType parentType = rootType;
                var length = detail.path().size();
                for (String path : detail.path()) {
                    length--;

                    ColumnType currentColumnType = new ObjectType();
                    ColumnName currentColumnName = null;

                    // check if currentColumn name prefix match any of parentType column names
                    // and if to take the column type from the parentType
                    for (var entry : parentType.entrySet()) {
                        var key = entry.getKey();
                        if (path.startsWith(key.columnName() + "_")) {
                            var cleanPath = path.substring(0, key.columnName().length());
                            var info3 = parentType.tryGetColumnNamed(new ColumnName(cleanPath));
                            if (!info3.isEmpty()) {
                                currentColumnName = info3.get().getRight().columnName();
                                break;
                            }
                        }
                    }

                    // if is last then take the column type from the column
                    boolean isLast = length == 0;
                    if (isLast) {
                        currentColumnType = columnType;
                    }

                    if (currentColumnName == null) {
                        currentColumnName = ColumnName.normalized(path, currentColumnType);
                    }

                    var info2 = parentType.tryGetColumnNamed(ColumnName.normalized(path, currentColumnType));
                    // var info2 = parentType.tryGetColumnInfoOf(ColumnName.normalized(path, currentColumnType), currentColumnType);
                    if (!info2.isEmpty()) {
                        currentColumnType = info2.get().getLeft();
                    }

                    if (currentColumnType instanceof ArrayType at) {
                        if (at.elementType() instanceof ObjectType ot) {
                            parentType.putColumnNameWithType2(currentColumnName, at);
                            parentType = ot;
                            continue;
                        }
                    }

                    // ObjectType current = ObjectType.of(currentColumnName, currentColumnType);
                    parentType.putColumnNameWithType2(currentColumnName, currentColumnType);
                    if (!isLast) {
                        if (currentColumnType instanceof ObjectType ot) {
                            parentType = ot;
                        }
                        else {
                            parentType = new ObjectType();
                        }
                    }
                }
                schema.putColumnNameWithType2(columnName, rootType);
            }
            else {
                schema.putColumnNameWithType2(columnName, columnType);
            }
        }
    }

    private static ColumnType getColumnType(InformationSchemaColumnInfo column) {
        return switch (column.dataType()) {
            case "smallint", "bigint", "integer" -> new BigIntType();
            case "double precision", "real" -> new FloatType();
            case "timestamp with time zone", "timestamp without time zone" -> new TimezType();
            case "bit" -> new BitType(column.characterMaximumLength());
            case "ip", "text" -> new TextType();
            case "object" -> new ObjectType();
            case "boolean" -> new BooleanType();
            case "character" -> new CharType(column.characterMaximumLength());
            case "float_vector" -> new ArrayType(new FloatType());
            case "geo_point", "geo_shape" -> new GeoShapeType();
            default -> {
                if (column.isArray()) {
                    yield new ArrayType(getColumnType(column.subArray()));
                }

                throw new IllegalArgumentException("Unknown data type: " + column.dataType());
            }
        };
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
                    yield fromObject(json, schema);
                } catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    OffsetDateTime time = OffsetDateTime.parse(x);
                    yield time;
                } catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    ZonedDateTime time = ZonedDateTime.parse(x);
                    yield time;
                } catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    LocalTime time = LocalTime.parse(x);
                    yield time;
                } catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    LocalDateTime time = LocalDateTime.parse(x);
                    yield time;
                } catch (Exception ignored) {
                    // DO nothing
                }

                yield x;
            }

            case List x -> {
                if (isEmptyList(x)) {
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
                    if (isEmptyList(value)) {
                        continue;
                    }

                    // if is empty array, skip
                    if (value instanceof List<?> list) {
                        if (isPolyList(list)) {
                            // split list by types into multiple columns
                            var splits = splitByType(list);

                            for (var split : splits.entrySet()) {
                                var value2 = split.getValue();
                                ColumnType columnType = split.getKey();
                                ColumnName columnName2 = ColumnName.normalized(key.toString(), columnType);
                                var info = schema.putColumnNameWithType2(columnName2, columnType);

                                if (info.getLeft() instanceof ObjectType oo) {
                                    var val = fromObject(value2, oo);
                                    result.put(info.getRight().columnName().columnName(), val);
                                } else {
                                    // most likely here we deal with primitive type
                                    var val = fromObject(value2, new ObjectType());
                                    result.put(info.getRight().columnName().columnName(), val);
                                }
                            }
                            continue;
                        }
                    }

                    ColumnType columnType2 = detect(value);
                    ColumnName columnName2 = ColumnName.normalized(key.toString(), columnType2);
                    var info = schema.putColumnNameWithType2(columnName2, columnType2);

                    if (info.getLeft() instanceof ObjectType oo) {
                        var val = fromObject(value, oo);
                        result.put(info.getRight().columnName().columnName(), val);
                    } else {
                        // most likely here we deal with primitive type
                        var val = fromObject(value, new ObjectType());
                        result.put(info.getRight().columnName().columnName(), val);
                    }
                }

                yield result;
            }
            default -> o;
        };
    }

    private static boolean isEmptyList(Object list) {
        if (!(list instanceof List<?>)) {
            return false;
        }

        while (list instanceof List<?> l) {
            if (l.isEmpty()) {
                return true;
            }

            list = l.get(0);
        }

        return false;
    }

    private Map<ColumnType, List<Object>> splitByType(List<?> list) {
        Map<ColumnType, List<Object>> result = new LinkedHashMap<>();
        for (Object o : list) {
            if (o instanceof List<?> l) {
                if (isEmptyList(o)) {
                    continue;
                }

                if (isPolyList(o)) {
                    splitByType(l).forEach((k, v) -> {
                        k = new ArrayType(k);
                        if (result.containsKey(k)) {
                            result.get(k).addAll(v);
                        }
                        else {
                            result.put(k, v);
                        }
                    });
                    continue;
                }
            }

            ColumnType columnType = detect(o);
            columnType = new ArrayType(columnType);
            if (result.containsKey(columnType)) {
                result.get(columnType).add(o);
            }
            else {
                List<Object> newList = new ArrayList<>();
                newList.add(o);
                result.put(columnType, newList);
            }
        }

        return result;
    }

    private boolean isPolyList(Object o) {
        if (o instanceof List<?> list) {
            return list.stream().anyMatch(x -> !x.getClass().equals(list.get(0).getClass()));
        }

        return false;
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
                } catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    OffsetDateTime time = OffsetDateTime.parse(x);
                    yield detect(time);
                } catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    ZonedDateTime time = ZonedDateTime.parse(x);
                    yield detect(time);
                } catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    LocalTime time = LocalTime.parse(x);
                    yield detect(time);
                } catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    LocalDateTime time = LocalDateTime.parse(x);
                    yield detect(time);
                } catch (Exception ignored) {
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
            case Boolean x -> new BooleanType();
            case Float x -> new FloatType();
            case Double x -> new FloatType();
            case java.sql.Timestamp x -> new TimezType();
            case java.time.Instant x -> new TimezType();
            case java.time.OffsetDateTime x -> new TimezType();
            case java.time.ZonedDateTime x -> new TimezType();
            case java.time.LocalDateTime x -> new TimezType();
            case java.time.LocalDate x -> new TimezType();
            case java.time.LocalTime x -> new TimezType();
            case java.net.InetAddress x -> new TextType();

            case List x -> {
                if (isEmptyList(x)) {
                    throw new RuntimeException("Empty array");
                }

                if (isPolyList(x)) {
                    throw new RuntimeException("Mixed array");
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
                    if (isEmptyList(x.get(key))) {
                        continue;
                    }

                    ColumnType valueType = detect(x.get(key));
                    ObjectType current = (ObjectType) ObjectType.of(columnName, valueType);
                    result.merge(current);
                }

                yield result;
            }

            default -> throw new IllegalStateException("Unexpected value: " + o);
        };
    }

    public ColumnInfo addColumn(ColumnName columnName, ColumnType columnType) {
        return schema.putColumnNameWithType(columnName, columnType);
    }

    public void putCollision(ColumnName name, TypeCollision collision) {
        this.schema.put(name, collision);
    }

    public ObjectType getObjectType(ColumnName columnName) {
        var result = schema.tryGetColumnInfoOfObjectType(columnName);
        if (result.isPresent()) {
            return result.get().getLeft();
        }

        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    public String typeName(ColumnType c) {
        return switch (c) {
            case BigIntType() -> "BIGINT";
            case TextType() -> "TEXT";
            case BooleanType() -> "BOOLEAN";
            case FloatType() -> "REAL";
            case TimezType() -> "TIMETZ";
            case GeoShapeType() -> "GEO_SHAPE";
            case CharType(Number size) -> "CHAR(" + size + ")";
            case BitType(Number size) -> "BIT(" + size + ")";
            case ArrayType(ColumnType elementType) -> "ARRAY[" + typeName(elementType) + "]";
            case ObjectType ot -> "OBJECT";
        };
    }

    public String typeShape(ColumnType c) {
        return switch (c) {
            case BigIntType() -> typeName(c);
            case TextType() -> typeName(c);
            case BooleanType() -> typeName(c);
            case FloatType() -> typeName(c);
            case TimezType() -> typeName(c);
            case GeoShapeType() -> typeName(c);
            case CharType(Number size) -> typeName(c);
            case BitType(Number size) -> typeName(c);
            case ArrayType(ColumnType elementType) -> "ARRAY[" + typeShape(elementType) + "]";
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

    public ObjectType getSchema() {
        return schema;
    }
}
