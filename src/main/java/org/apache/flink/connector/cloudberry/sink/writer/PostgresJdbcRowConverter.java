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

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Serializable;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;

import org.apache.flink.connector.cloudberry.common.JdbcTypeUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

/** Base class for all converters that convert between JDBC object and Flink internal object. */
public final class PostgresJdbcRowConverter implements JdbcRowConverter {

  protected final RowType rowType;
  protected final JdbcSerializationConverter[] toExternalConverters;
  protected final LogicalType[] fieldTypes;

  public PostgresJdbcRowConverter(RowType rowType) {
    this.rowType = checkNotNull(rowType);
    this.fieldTypes =
        rowType.getFields().stream().map(RowType.RowField::getType).toArray(LogicalType[]::new);

    this.toExternalConverters = new JdbcSerializationConverter[rowType.getFieldCount()];
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      toExternalConverters[i] = createNullableExternalConverter(fieldTypes[i]);
    }
  }

  @Override
  public BatchWriter toExternal(RowData rowData, BatchWriter statement) throws SQLException {
    for (int index = 0; index < rowData.getArity(); index++) {
      toExternalConverters[index].serialize(rowData, index, statement);
    }
    return statement;
  }

  /**
   * Runtime converter to convert {@link RowData} field to java object and fill into the {@link
   * PreparedStatement}.
   */
  @FunctionalInterface
  public interface JdbcSerializationConverter extends Serializable {
    void serialize(RowData rowData, int index, BatchWriter statement) throws SQLException;
  }

  /** Create a nullable JDBC f{@link JdbcSerializationConverter} from given sql type. */
  protected JdbcSerializationConverter createNullableExternalConverter(LogicalType type) {
    return wrapIntoNullableExternalConverter(createExternalConverter(type), type);
  }

  protected JdbcSerializationConverter wrapIntoNullableExternalConverter(
      JdbcSerializationConverter jdbcSerializationConverter, LogicalType type) {
    final int sqlType = JdbcTypeUtil.logicalTypeToSqlType(type.getTypeRoot());
    return (val, index, statement) -> {
      if (val == null || val.isNullAt(index) || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
        statement.setNull(index, sqlType);
      } else {
        jdbcSerializationConverter.serialize(val, index, statement);
      }
    };
  }

  protected JdbcSerializationConverter createExternalConverter(LogicalType type) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
        return (val, index, statement) -> statement.setBoolean(index, val.getBoolean(index));
      case TINYINT:
        return (val, index, statement) -> statement.setByte(index, val.getByte(index));
      case SMALLINT:
        return (val, index, statement) -> statement.setShort(index, val.getShort(index));
      case INTEGER:
      case INTERVAL_YEAR_MONTH:
        return (val, index, statement) -> statement.setInt(index, val.getInt(index));
      case BIGINT:
      case INTERVAL_DAY_TIME:
        return (val, index, statement) -> statement.setLong(index, val.getLong(index));
      case FLOAT:
        return (val, index, statement) -> statement.setFloat(index, val.getFloat(index));
      case DOUBLE:
        return (val, index, statement) -> statement.setDouble(index, val.getDouble(index));
      case CHAR:
      case VARCHAR:
        // value is BinaryString
        return (val, index, statement) ->
            statement.setString(index, val.getString(index).toString());
      case BINARY:
      case VARBINARY:
        return (val, index, statement) -> statement.setBytes(index, val.getBinary(index));
      case DATE:
        return (val, index, statement) ->
            statement.setDate(index, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
      case TIME_WITHOUT_TIME_ZONE:
        return (val, index, statement) ->
            statement.setTime(
                index, Time.valueOf(LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L)));
      case TIMESTAMP_WITH_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        final int timestampPrecision = ((TimestampType) type).getPrecision();
        return (val, index, statement) ->
            statement.setTimestamp(
                index, val.getTimestamp(index, timestampPrecision).toTimestamp());
      case DECIMAL:
        final int decimalPrecision = ((DecimalType) type).getPrecision();
        final int decimalScale = ((DecimalType) type).getScale();
        return (val, index, statement) ->
            statement.setBigDecimal(
                index, val.getDecimal(index, decimalPrecision, decimalScale).toBigDecimal());
      case ARRAY:
      case MAP:
      case MULTISET:
      case ROW:
      case RAW:
      default:
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
  }
}
