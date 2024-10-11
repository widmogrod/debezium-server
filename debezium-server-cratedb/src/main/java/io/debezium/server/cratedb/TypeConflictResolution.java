/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

public class TypeConflictResolution {
    final public static String NAME_SILENT_NULL = "null";
    final public static String NAME_STORE_MALFORMED_FRAGMENTS = "malformed";
    final public static String TYPE_SUFFIX_AND_MALFORMED = "type_suffix_and_malformed";

    public enum Strategy {
        SILENT_NULL,
        STORE_MALFORMED_FRAGMENTS,
        TYPE_SUFFIX_AND_MALFORMED;
    }

    public static String stringify(Strategy strategies) {
        return switch (strategies) {
            case SILENT_NULL -> NAME_SILENT_NULL;
            case STORE_MALFORMED_FRAGMENTS -> NAME_STORE_MALFORMED_FRAGMENTS;
            case TYPE_SUFFIX_AND_MALFORMED -> TYPE_SUFFIX_AND_MALFORMED;
        };
    }

    public static Strategy fromString(String strategyName) {
        return switch (strategyName) {
            case NAME_SILENT_NULL -> Strategy.SILENT_NULL;
            case TYPE_SUFFIX_AND_MALFORMED -> Strategy.TYPE_SUFFIX_AND_MALFORMED;
            default -> Strategy.STORE_MALFORMED_FRAGMENTS;
        };
    }
}
