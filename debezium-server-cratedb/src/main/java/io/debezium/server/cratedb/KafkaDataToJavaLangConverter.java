/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.data.Json;

/**
 * Implementation of conversion of KafkaData to Java objects
 *
 * @author Gabriel Habryn
 */
public class KafkaDataToJavaLangConverter {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Object convertToJavaObject(Schema schema, Object value) {
        if (schema == null || value == null) {
            return null;
        }

        return switch (schema.type()) {
            case INT8, INT16, INT32, INT64 -> switch (value) {
                case Date date -> date.getTime();
                case Number number -> number.longValue();
                default -> value;
            };
            case FLOAT32 -> ((Number) value).floatValue();
            case FLOAT64 -> ((Number) value).doubleValue();
            case BOOLEAN -> (boolean) value;
            case STRING -> switch (schema.name()) {
                case Json.LOGICAL_NAME -> {
                    try {
                        yield objectMapper.readValue(value.toString(), Object.class);
                    }
                    catch (JsonProcessingException e) {
                        yield value.toString();
                    }
                }
                case null, default -> value.toString();
            };

            case BYTES -> switch (value) {
                case BigDecimal bigDecimal -> bigDecimal.toPlainString();
                default -> java.util.Base64.getEncoder().encodeToString((byte[]) value);
            };
            case ARRAY -> value instanceof List<?> list ? convertArray(schema, list) : null;
            case MAP -> value instanceof Map<?, ?> map ? convertMap(schema, map) : null;
            case STRUCT -> value instanceof Struct struct ? convertStruct(schema, struct) : null;
        };
    }

    private static Object convertArray(Schema schema, List<?> value) {
        Schema elementSchema = schema.valueSchema();
        List<Object> list = new ArrayList<>();
        for (Object element : value) {
            list.add(convertToJavaObject(elementSchema, element));
        }
        return list;
    }

    private static Object convertMap(Schema schema, Map<?, ?> value) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            Object key = convertToJavaObject(schema.keySchema(), entry.getKey());
            Object val = convertToJavaObject(schema.valueSchema(), entry.getValue());
            map.put(key, val);
        }
        return map;
    }

    private static Object convertStruct(Schema schema, Struct value) {
        // if is "debezium array"
        // https://debezium.io/documentation/reference/stable/transformations/mongodb-event-flattening.html#mongodb-event-flattening-array-encoding
        var isDebeziumArray = schema.fields()
                .stream()
                .allMatch(key -> key.name().equals("_" + key.index()));

        if (isDebeziumArray) {
            // convert to array
            List<Object> list = new ArrayList<>();
            for (Field field : schema.fields()) {
                Object val = value.get(field.name());
                list.add(convertToJavaObject(field.schema(), val));
            }
            return list;
        }

        Map<String, Object> map = new LinkedHashMap<>();
        for (Field field : schema.fields()) {
            Object val = value.get(field.name());
            map.put(field.name(), convertToJavaObject(field.schema(), val));
        }
        return map;
    }
}
