/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

 package marquez.db.mappers;

 import com.google.common.collect.ImmutableList;
 import java.sql.ResultSet;
 import java.sql.SQLException;
 import java.util.Arrays;
 import java.util.UUID;
 import java.util.stream.Collectors;
 import lombok.extern.slf4j.Slf4j;
 import marquez.db.models.ColumnLineageNodeData;
 import marquez.db.models.InputFieldNodeData;
 import org.jdbi.v3.core.mapper.RowMapper;
 import org.jdbi.v3.core.statement.StatementContext;
 import org.postgresql.jdbc.PgArray;
 
 @Slf4j
 public class ColumnLineageNodeDataMapper implements RowMapper<ColumnLineageNodeData> {
 
   @Override
   public ColumnLineageNodeData map(ResultSet rs, StatementContext ctx) throws SQLException {
     // Get the inputFields array from the result set
     PgArray inputFieldsArray = (PgArray) rs.getArray("inputFields");
     Object[] inputFields = (Object[]) inputFieldsArray.getArray();
     
     // Convert each input field array into an InputFieldNodeData
     ImmutableList<InputFieldNodeData> inputFieldNodes = Arrays.stream(inputFields)
         .map(field -> (String[]) field)
         .map(fieldArray -> new InputFieldNodeData(
             fieldArray[0], // namespace_name
             fieldArray[1], // dataset_name
             UUID.fromString(fieldArray[2]), // input_dataset_version_uuid
             fieldArray[3], // field_name
             fieldArray[4], // transformation_description
             fieldArray[5]  // transformation_type
         ))
         .collect(ImmutableList.toImmutableList());
 
     // Create ColumnLineageNodeData using the input fields
     return new ColumnLineageNodeData(
         rs.getString("namespace_name"),
         rs.getString("dataset_name"),
         rs.getString("dataset_version_uuid") != null ? UUID.fromString(rs.getString("dataset_version_uuid")) : null,
         rs.getString("field_name"),
         rs.getString("type"),
         inputFieldNodes
     );
   }
 
   public static ImmutableList<InputFieldNodeData> toInputFields(ResultSet results, String column)
       throws SQLException {
     if (results.getObject(column) == null) {
       return ImmutableList.of();
     }
 
     PgArray pgArray = (PgArray) results.getObject(column);
     Object[] deserializedArray = (Object[]) pgArray.getArray();
 
     return ImmutableList.copyOf(
         Arrays.asList(deserializedArray).stream()
             .map(o -> (String[]) o)
             .map(
                 arr ->
                     new InputFieldNodeData(
                         arr[0], arr[1], UUID.fromString(arr[2]), arr[3], arr[4], arr[5]))
             .collect(Collectors.toList()));
   }
 }
 