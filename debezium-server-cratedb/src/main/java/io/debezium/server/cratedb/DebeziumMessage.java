/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumMessage {
    private DebeziumMessagePayload payload;

    public DebeziumMessage(@JsonProperty("payload") DebeziumMessagePayload payload) {
        this.payload = payload;
    }

    public DebeziumMessagePayload getPayload() {
        return payload;
    }
}
