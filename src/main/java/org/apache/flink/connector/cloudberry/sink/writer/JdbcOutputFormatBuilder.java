/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.cloudberry.sink.writer;

import static org.apache.flink.table.data.RowData.createFieldGetter;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.cloudberry.common.JdbcDmlOptions;
import org.apache.flink.connector.cloudberry.common.JdbcQueryBuilder;
import org.apache.flink.connector.cloudberry.common.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.cloudberry.common.config.JdbcConnectionOptions;
import org.apache.flink.connector.cloudberry.sink.config.JdbcWriteOptions;
import org.apache.flink.connector.cloudberry.sink.writer.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.cloudberry.sink.writer.executor.TableBufferReducedStatementExecutor;
import org.apache.flink.connector.cloudberry.sink.writer.executor.TableBufferedStatementExecutor;
import org.apache.flink.connector.cloudberry.sink.writer.executor.TableSimpleStatementExecutor;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** Builder for {@link JdbcOutputFormat} for Table/SQL. */
public class JdbcOutputFormatBuilder implements Serializable {

  private static final long serialVersionUID = 1L;

  private JdbcConnectionOptions jdbcOptions;
  private JdbcWriteOptions writeOptions;
  private JdbcDmlOptions dmlOptions;
  private TypeInformation<RowData> rowDataTypeInformation;
  private DataType[] fieldDataTypes;

  public JdbcOutputFormatBuilder() {}

  public JdbcOutputFormatBuilder setJdbcOptions(JdbcConnectionOptions jdbcOptions) {
    this.jdbcOptions = jdbcOptions;
    return this;
  }

  public JdbcOutputFormatBuilder setJdbcWriteOptions(JdbcWriteOptions writeOptions) {
    this.writeOptions = writeOptions;
    return this;
  }

  public JdbcOutputFormatBuilder setJdbcDmlOptions(JdbcDmlOptions dmlOptions) {
    this.dmlOptions = dmlOptions;
    return this;
  }

  public JdbcOutputFormatBuilder setRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
    this.rowDataTypeInformation = rowDataTypeInfo;
    return this;
  }

  public JdbcOutputFormatBuilder setFieldDataTypes(DataType[] fieldDataTypes) {
    this.fieldDataTypes = fieldDataTypes;
    return this;
  }

  public JdbcOutputFormat<RowData, ?, ?> build() {
    checkNotNull(jdbcOptions, "jdbc options can not be null");
    checkNotNull(dmlOptions, "jdbc dml options can not be null");
    checkNotNull(writeOptions, "jdbc write options can not be null");

    final LogicalType[] logicalTypes =
        Arrays.stream(fieldDataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);

    if (dmlOptions.getKeyFields().isPresent() && dmlOptions.getKeyFields().get().length > 0) {
      // upsert query
      // Note: Don't capture queryBuilder in lambda to avoid serialization issues
      return new JdbcOutputFormat<>(
          new SimpleJdbcConnectionProvider(jdbcOptions),
          writeOptions,
          ctx ->
              createBufferReduceExecutor(
                  dmlOptions, JdbcQueryBuilder.getInstance(), rowDataTypeInformation, logicalTypes),
          JdbcOutputFormat.RecordExtractor.identity());
    } else {
      // append only query
      // Generate SQL string here to avoid capturing queryBuilder
      final String sql =
          JdbcQueryBuilder.getInstance()
              .getInsertIntoStatement(dmlOptions.getTableName(), dmlOptions.getFieldNames());
      return new JdbcOutputFormat<>(
          new SimpleJdbcConnectionProvider(jdbcOptions),
          writeOptions,
          ctx ->
              createSimpleBufferedExecutor(
                  dmlOptions.getFieldNames(),
                  logicalTypes,
                  writeOptions.isUseCopyProtocol() == true ? dmlOptions.getTableName() : sql,
                  writeOptions.isUseCopyProtocol()),
          JdbcOutputFormat.RecordExtractor.identity());
    }
  }

  private static JdbcBatchStatementExecutor<RowData> createBufferReduceExecutor(
      JdbcDmlOptions opt,
      JdbcQueryBuilder queryBuilder,
      TypeInformation<RowData> rowDataTypeInfo,
      LogicalType[] fieldTypes) {
    checkArgument(opt.getKeyFields().isPresent());
    String tableName = opt.getTableName();
    String[] pkNames = opt.getKeyFields().get();
    int[] pkFields =
        Arrays.stream(pkNames).mapToInt(Arrays.asList(opt.getFieldNames())::indexOf).toArray();
    LogicalType[] pkTypes =
        Arrays.stream(pkFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);

    return new TableBufferReducedStatementExecutor(
        createUpsertRowExecutor(
            queryBuilder, tableName, opt.getFieldNames(), fieldTypes, pkFields, pkNames, pkTypes),
        createDeleteExecutor(queryBuilder, tableName, pkNames, pkTypes),
        createRowKeyExtractor(fieldTypes, pkFields));
  }

  private static JdbcBatchStatementExecutor<RowData> createSimpleBufferedExecutor(
      String[] fieldNames, LogicalType[] fieldTypes, String sql, boolean useCopyProtocol) {
    return new TableBufferedStatementExecutor(
        createSimpleRowExecutor(fieldNames, fieldTypes, sql, useCopyProtocol));
  }

  private static JdbcBatchStatementExecutor<RowData> createUpsertRowExecutor(
      JdbcQueryBuilder queryBuilder,
      String tableName,
      String[] fieldNames,
      LogicalType[] fieldTypes,
      int[] pkFields,
      String[] pkNames,
      LogicalType[] pkTypes) {
    String upsertSql = queryBuilder.getUpsertStatement(tableName, fieldNames, pkNames);
    return createSimpleRowExecutor(fieldNames, fieldTypes, upsertSql, false);
  }

  private static JdbcBatchStatementExecutor<RowData> createDeleteExecutor(
      JdbcQueryBuilder queryBuilder, String tableName, String[] pkNames, LogicalType[] pkTypes) {
    String deleteSql = queryBuilder.getDeleteStatement(tableName, pkNames);
    return createSimpleRowExecutor(pkNames, pkTypes, deleteSql, false);
  }

  private static JdbcBatchStatementExecutor<RowData> createSimpleRowExecutor(
      String[] fieldNames, LogicalType[] fieldTypes, final String sql, boolean useCopyProtocol) {
    final JdbcRowConverter converter = new PostgresJdbcRowConverter(RowType.of(fieldTypes));
    return new TableSimpleStatementExecutor(
        connection ->
            BatchWriterFactory.createBatchWriter(connection, sql, fieldNames, useCopyProtocol),
        converter);
  }

  private static Function<RowData, RowData> createRowKeyExtractor(
      LogicalType[] logicalTypes, int[] pkFields) {
    final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[pkFields.length];
    for (int i = 0; i < pkFields.length; i++) {
      fieldGetters[i] = createFieldGetter(logicalTypes[pkFields[i]], pkFields[i]);
    }
    return row -> getPrimaryKey(row, fieldGetters);
  }

  private static RowData getPrimaryKey(RowData row, RowData.FieldGetter[] fieldGetters) {
    GenericRowData pkRow = new GenericRowData(fieldGetters.length);
    for (int i = 0; i < fieldGetters.length; i++) {
      pkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
    }
    return pkRow;
  }
}
