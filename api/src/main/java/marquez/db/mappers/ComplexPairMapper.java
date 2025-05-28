/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.mappers;

import static marquez.db.Columns.stringOrThrow;
import static marquez.db.Columns.uuidOrThrow;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import lombok.NonNull;
import marquez.db.Columns;
import org.apache.commons.lang3.tuple.Pair;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

public final class ComplexPairMapper implements RowMapper<Pair<Pair<String, Pair<String, String>>, UUID>> {
    @Override
    public Pair<Pair<String, Pair<String, String>>, UUID> map(@NonNull ResultSet results, @NonNull StatementContext context)
            throws SQLException {
        String namespaceName = stringOrThrow(results, "namespace_name");
        String datasetName = stringOrThrow(results, "dataset_name");
        String fieldName = stringOrThrow(results, "name");
        UUID uuid = uuidOrThrow(results, Columns.ROW_UUID);

        return Pair.of(
            Pair.of(namespaceName, Pair.of(datasetName, fieldName)),
            uuid
        );
    }
} 