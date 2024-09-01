/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Converts Java types to CrateDB types.
 *
 * @author Gabriel Habryn
 */
public class CrateDBType {
    private final Type type;
    private String extra = null;

    public CrateDBType(Type type) {
        this.type = type;
    }

    public CrateDBType(Type type, String extra) {
        this.type = type;
        this.extra = extra;
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

    private static final DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ofPattern("[MM/dd/yyyy]" + "[dd-MM-yyyy]" + "[yyyy-MM-dd]"))
            .append(DateTimeFormatter.ofPattern("[HH:mm:ss]"))
            .toFormatter();

    private static final String NUMBER_REGEX = "[+-]?(\\d+([.]\\d*)?(e[+-]?\\d+)?|[.]\\d+(e[+-]?\\d+)?)";
    private static final String WHITESPACES_REGEX = "\\s*";
    private static final String POINT_REGEX = "POINT";

    private static final Pattern pointPattern = Pattern.compile(
            POINT_REGEX + WHITESPACES_REGEX + "\\(" + NUMBER_REGEX + WHITESPACES_REGEX + NUMBER_REGEX + "\\)");

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
                } catch (Exception ignored) {
                }

                try {
                    OffsetDateTime time = OffsetDateTime.parse(x);
                    yield from(time);
                } catch (Exception ignored) {
                }

                try {
                    ZonedDateTime time = ZonedDateTime.parse(x);
                    yield from(time);
                } catch (Exception ignored) {
                }

                try {
                    LocalTime time = LocalTime.parse(x);
                    yield from(time);
                } catch (Exception ignored) {
                }

                try {
                    LocalDateTime time = LocalDateTime.parse(x);
                    yield from(time);
                } catch (Exception ignored) {
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
}

