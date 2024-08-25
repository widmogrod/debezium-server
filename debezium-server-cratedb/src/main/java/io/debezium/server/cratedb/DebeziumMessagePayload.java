/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumMessagePayload {
    private String op;
    private Object after;

    public DebeziumMessagePayload(@JsonProperty("op") String op, @JsonProperty("after") Object after) {
        this.op = op;
        this.after = after;
    }

    public String getOp() {
        return op;
    }

    public Object getAfter() {
        return after;
    }
}
