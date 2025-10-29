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

package org.apache.flink.connector.cloudberry.table;

import static org.apache.flink.util.Preconditions.checkState;

import java.util.Objects;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.cloudberry.common.JdbcDmlOptions;
import org.apache.flink.connector.cloudberry.common.config.JdbcConnectionOptions;
import org.apache.flink.connector.cloudberry.sink.GenericJdbcSinkFunction;
import org.apache.flink.connector.cloudberry.sink.config.JdbcWriteOptions;
import org.apache.flink.connector.cloudberry.sink.writer.JdbcOutputFormatBuilder;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

/** A {@link DynamicTableSink} for JDBC. */
@Internal
public class JdbcDynamicTableSink implements DynamicTableSink {

  private final JdbcConnectionOptions jdbcOptions;
  private final JdbcWriteOptions writeOptions;
  private final JdbcDmlOptions dmlOptions;
  private final DataType physicalRowDataType;

  public JdbcDynamicTableSink(
      JdbcConnectionOptions jdbcOptions,
      JdbcWriteOptions writeOptions,
      JdbcDmlOptions dmlOptions,
      DataType physicalRowDataType) {
    this.jdbcOptions = jdbcOptions;
    this.writeOptions = writeOptions;
    this.dmlOptions = dmlOptions;
    this.physicalRowDataType = physicalRowDataType;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    validatePrimaryKey(requestedMode);
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.DELETE)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build();
  }

  private void validatePrimaryKey(ChangelogMode requestedMode) {
    checkState(
        ChangelogMode.insertOnly().equals(requestedMode) || dmlOptions.getKeyFields().isPresent(),
        "please declare primary key for sink table when query contains update/delete record.");
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    final TypeInformation<RowData> rowDataTypeInformation =
        context.createTypeInformation(physicalRowDataType);
    final JdbcOutputFormatBuilder builder = new JdbcOutputFormatBuilder();

    builder.setJdbcOptions(jdbcOptions);
    builder.setJdbcDmlOptions(dmlOptions);
    builder.setJdbcWriteOptions(writeOptions);
    builder.setRowDataTypeInfo(rowDataTypeInformation);
    builder.setFieldDataTypes(
        DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]));
    return SinkFunctionProvider.of(
        new GenericJdbcSinkFunction<>(builder.build()), jdbcOptions.getParallelism());
  }

  @Override
  public DynamicTableSink copy() {
    return new JdbcDynamicTableSink(jdbcOptions, writeOptions, dmlOptions, physicalRowDataType);
  }

  @Override
  public String asSummaryString() {
    return "JDBC:Cloudberry";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JdbcDynamicTableSink)) {
      return false;
    }
    JdbcDynamicTableSink that = (JdbcDynamicTableSink) o;
    return Objects.equals(jdbcOptions, that.jdbcOptions)
        && Objects.equals(writeOptions, that.writeOptions)
        && Objects.equals(dmlOptions, that.dmlOptions)
        && Objects.equals(physicalRowDataType, that.physicalRowDataType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jdbcOptions, writeOptions, dmlOptions, physicalRowDataType);
  }
}
