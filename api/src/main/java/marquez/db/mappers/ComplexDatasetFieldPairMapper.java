/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.mappers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

public final class ComplexDatasetFieldPairMapper implements RowMapper<Pair<Pair<String, Pair<String, String>>, UUID>> {
    @Override
    public Pair<Pair<String, Pair<String, String>>, UUID> map(@NonNull ResultSet results, @NonNull StatementContext context)
            throws SQLException {
        // Map the columns to the complex Pair structure
        String namespaceName = results.getString("namespace_name");
        String datasetName = results.getString("dataset_name");
        String fieldName = results.getString("name");
        UUID uuid = results.getObject("uuid", UUID.class);

        // Construct the nested Pair structure
        Pair<String, String> datasetPair = Pair.of(datasetName, fieldName);
        Pair<String, Pair<String, String>> identifierPair = Pair.of(namespaceName, datasetPair);
        
        // Return the final Pair with UUID
        return Pair.of(identifierPair, uuid);
    }
} 