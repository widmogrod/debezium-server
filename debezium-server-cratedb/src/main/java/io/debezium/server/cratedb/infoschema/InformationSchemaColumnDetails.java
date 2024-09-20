/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.infoschema;

import java.util.List;

// Nested record for column_details
public record InformationSchemaColumnDetails(String name, List<String> path) {
}
