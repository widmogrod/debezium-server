/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Loads schema information from CrateDB.
 *
 * @author Gabriel Habryn
 */
public class InformationSchemaLoader {
    public static final String INFO_SQL = """
            SELECT c.column_details
                 , c.column_name
                 , c.data_type
                 , c.character_maximum_length
                 , IF(k.column_name IS NOT NULL, true, false) AS is_primary_key
            FROM information_schema.columns AS c
            LEFT JOIN information_schema.key_column_usage AS k
              ON c.table_name = k.table_name
                  AND c.column_name = k.column_name
                  AND c.table_catalog = k.table_catalog
                  AND c.table_schema = k.table_schema
            WHERE c.table_name = ?;
            """;

    private final String tableName;
    private final ObjectMapper mapper = new ObjectMapper();

    private InformationSchemaLoader(String tableName) {
        this.tableName = tableName;
    }

    public static InformationSchemaLoader withTableName(String tableName) {
        return new InformationSchemaLoader(tableName);
    }

    public List<InformationSchemaColumnInfo> load(Connection conn) throws SQLException, JsonProcessingException {
        List<InformationSchemaColumnInfo> infos = new LinkedList<>();
        try (var stmt = conn.prepareStatement(INFO_SQL)) {
            stmt.setString(1, this.tableName);
            try (var resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    String json = resultSet.getString("column_details");

                    var details = mapper.readValue(json, InformationSchemaColumnDetails.class);

                    var info = new InformationSchemaColumnInfo.Builder().setColumnName(resultSet.getString("column_name")).setDataType(resultSet.getString("data_type"))
                            .setColumnDetails(details).setIsPrimaryKey(resultSet.getBoolean("is_primary_key"))
                            .setCharacterMaximumLength(resultSet.getInt("character_maximum_length")).build();

                    infos.add(info);
                }
            }
        }

        return infos;
    }
}
