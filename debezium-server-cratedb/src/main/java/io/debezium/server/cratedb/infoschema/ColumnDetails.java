/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.infoschema;

import java.util.List;

/**
 * Represents the details of a column in a database table.
 */
public record ColumnDetails(String name, List<String> path) {
}
