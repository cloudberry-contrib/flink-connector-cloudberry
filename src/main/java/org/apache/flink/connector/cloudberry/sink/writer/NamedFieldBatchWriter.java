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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Simple JDBC PreparedStatement-based implementation of {@link BatchWriter} with named field
 * support.
 */
public class NamedFieldBatchWriter implements BatchWriter {
  private final PreparedStatement statement;

  public NamedFieldBatchWriter(PreparedStatement statement, String[] fieldNames) {
    checkNotNull(statement, "statement must not be null");
    this.statement = statement;
  }

  @Override
  public void addBatch() throws Exception {
    statement.addBatch();
  }

  @Override
  public int[] executeBatch() throws Exception {
    return statement.executeBatch();
  }

  @Override
  public void setNull(int fieldIndex, int sqlType) throws SQLException {
    statement.setNull(fieldIndex + 1, sqlType);
  }

  @Override
  public void setBoolean(int fieldIndex, boolean x) throws SQLException {
    statement.setBoolean(fieldIndex + 1, x);
  }

  @Override
  public void setByte(int fieldIndex, byte x) throws SQLException {
    statement.setByte(fieldIndex + 1, x);
  }

  @Override
  public void setShort(int fieldIndex, short x) throws SQLException {
    statement.setShort(fieldIndex + 1, x);
  }

  @Override
  public void setInt(int fieldIndex, int x) throws SQLException {
    statement.setInt(fieldIndex + 1, x);
  }

  @Override
  public void setLong(int fieldIndex, long x) throws SQLException {
    statement.setLong(fieldIndex + 1, x);
  }

  @Override
  public void setFloat(int fieldIndex, float x) throws SQLException {
    statement.setFloat(fieldIndex + 1, x);
  }

  @Override
  public void setDouble(int fieldIndex, double x) throws SQLException {
    statement.setDouble(fieldIndex + 1, x);
  }

  @Override
  public void setBigDecimal(int fieldIndex, BigDecimal x) throws SQLException {
    statement.setBigDecimal(fieldIndex + 1, x);
  }

  @Override
  public void setString(int fieldIndex, String x) throws SQLException {
    statement.setString(fieldIndex + 1, x);
  }

  @Override
  public void setBytes(int fieldIndex, byte[] x) throws SQLException {
    statement.setBytes(fieldIndex + 1, x);
  }

  @Override
  public void setDate(int fieldIndex, Date x) throws SQLException {
    statement.setDate(fieldIndex + 1, x);
  }

  @Override
  public void setTime(int fieldIndex, Time x) throws SQLException {
    statement.setTime(fieldIndex + 1, x);
  }

  @Override
  public void setTimestamp(int fieldIndex, Timestamp x) throws SQLException {
    statement.setTimestamp(fieldIndex + 1, x);
  }

  @Override
  public void close() throws Exception {
    statement.close();
  }
}
