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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Converts Java types to CrateDB types.
 *
 * @author Gabriel Habryn
 */
public class CrateDBType {
    private static final DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ofPattern("[MM/dd/yyyy]" + "[dd-MM-yyyy]" + "[yyyy-MM-dd]")).append(DateTimeFormatter.ofPattern("[HH:mm:ss]")).toFormatter();
    private static final String NUMBER_REGEX = "[+-]?(\\d+([.]\\d*)?(e[+-]?\\d+)?|[.]\\d+(e[+-]?\\d+)?)";
    private static final String WHITESPACES_REGEX = "\\s*";
    private static final String POINT_REGEX = "POINT";
    private static final Pattern pointPattern = Pattern.compile(POINT_REGEX + WHITESPACES_REGEX + "\\(" + NUMBER_REGEX + WHITESPACES_REGEX + NUMBER_REGEX + "\\)");
    private final Type type;
    private String extra = null;

    public CrateDBType(Type type) {
        this.type = type;
    }

    public CrateDBType(Type type, String extra) {
        this.type = type;
        this.extra = extra;
    }

    public static CrateDBType from(Object o) {
        return switch (o) {
            case String x -> {
                if (x.isEmpty()) {
                    yield new CrateDBType(Type.TEXT);
                }

                ObjectMapper mapper = new ObjectMapper();
                try {
                    Object json = mapper.readValue(x, Object.class);
                    yield from(json);
                }
                catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    OffsetDateTime time = OffsetDateTime.parse(x);
                    yield from(time);
                }
                catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    ZonedDateTime time = ZonedDateTime.parse(x);
                    yield from(time);
                }
                catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    LocalTime time = LocalTime.parse(x);
                    yield from(time);
                }
                catch (Exception ignored) {
                    // DO nothing
                }

                try {
                    LocalDateTime time = LocalDateTime.parse(x);
                    yield from(time);
                }
                catch (Exception ignored) {
                    // DO nothing
                }

                Matcher pointMatcher = pointPattern.matcher(x);
                if (pointMatcher.matches()) {
                    yield new CrateDBType(Type.GEO_POINT);
                }

                yield new CrateDBType(Type.TEXT);
            }

            case Integer x -> new CrateDBType(Type.BIGINT);
            case Long x -> new CrateDBType(Type.BIGINT);
            case Float x -> new CrateDBType(Type.REAL);
            case Double x -> new CrateDBType(Type.REAL);
            case Boolean x -> new CrateDBType(Type.BOOLEAN);
            case java.sql.Timestamp x -> new CrateDBType(Type.TIMESTAMP);
            case java.time.Instant x -> new CrateDBType(Type.TIMESTAMP);
            case java.time.OffsetDateTime x -> new CrateDBType(Type.TIMESTAMPTZ);
            case java.time.ZonedDateTime x -> new CrateDBType(Type.TIMESTAMPTZ);
            case java.time.LocalDateTime x -> new CrateDBType(Type.TIMESTAMPWOZ);
            case java.time.LocalDate x -> new CrateDBType(Type.DATE);
            case java.time.LocalTime x -> new CrateDBType(Type.TIME);
            case java.net.InetAddress x -> new CrateDBType(Type.IP);

            case List x -> {
                if (x.isEmpty()) {
                    // TODO should we throw an exception here?
                    // we don't know what is the type of the array
                    yield new CrateDBType(Type.ARRAY, "TEXT");
                }

                yield new CrateDBType(Type.ARRAY, from(x.getFirst()).getColumnType());
            }

            case Map x -> {
                if (x.isEmpty()) {
                    yield new CrateDBType(Type.OBJECT);
                }

                // iterate over keys and values
                String columnType = "(";
                for (Object key : x.keySet()) {
                    columnType += key + " AS ";
                    columnType += from(x.get(key)).getColumnType();
                    columnType += ", ";
                }
                columnType = columnType.substring(0, columnType.length() - 2);
                columnType += ")";

                yield new CrateDBType(Type.OBJECT, columnType);
            }

            default -> throw new IllegalStateException("Unexpected value: " + o);
        };
    }

    public static Object wrap(Object o) {
        return switch (o) {
            case List x -> {
                List<Object> list = new ArrayList<>();
                for (Object item : (List<?>) x) {
                    list.add(wrap(item));
                }

                yield list;
            }

            case Map x -> {
                if (x.isEmpty()) {
                    yield x;
                }

                // create newMap with same types as x
                Map<Object, Object> newMap = new LinkedHashMap<>();
                for (Object key : x.keySet()) {
                    CrateDBType type = from(x.get(key));
                    String newKey = key + "_" + type.getShortName();

                    newMap.put(newKey, wrap(x.get(key)));
                }

                yield newMap;
            }

            default -> o;
        };
    }

    public String getColumnType() {
        return switch (type) {
            case TEXT, IP, BOOLEAN -> "TEXT";
            case BIGINT, DATE -> "BIGINT";
            case REAL -> "REAL";
            case TIME, TIMESTAMP, TIMESTAMPTZ, TIMESTAMPWOZ -> "TIMETZ";
            case BIT -> "BIT(" + extra + ")";
            case ARRAY -> "ARRAY(" + extra + ")";
            case OBJECT -> "OBJECT" + (extra != null ? " AS " + extra : "");
            case GEO_POINT -> "GEO_POINT";
        };
    }

    public String getShortName() {
        return switch (type) {
            case TEXT, IP, BOOLEAN -> "t";
            case BIGINT, DATE -> "i";
            case REAL -> "f";
            case TIME, TIMESTAMP, TIMESTAMPTZ, TIMESTAMPWOZ -> "t";
            case BIT -> "b";
            // arr_ + first string of extra lowercase
            case ARRAY -> "arr" + (!extra.isEmpty() ? "_" + extra.substring(0, 1).toLowerCase() : "");
            case OBJECT -> "o";
            case GEO_POINT -> "p";
        };
    }

    public enum Type {
        TEXT,
        BIGINT,
        REAL,
        BOOLEAN,
        DATE,
        TIME,
        TIMESTAMP,
        TIMESTAMPTZ,
        TIMESTAMPWOZ,
        IP,
        BIT,
        ARRAY,
        OBJECT,
        GEO_POINT;
    }
}
