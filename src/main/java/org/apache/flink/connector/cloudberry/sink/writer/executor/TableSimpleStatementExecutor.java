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

package org.apache.flink.connector.cloudberry.sink.writer.executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.flink.connector.cloudberry.sink.writer.BatchWriter;
import org.apache.flink.connector.cloudberry.sink.writer.JdbcRowConverter;
import org.apache.flink.connector.cloudberry.sink.writer.StatementFactory;
import org.apache.flink.table.data.RowData;

/**
 * A {@link JdbcBatchStatementExecutor} that simply adds the records into batches of {@link
 * java.sql.PreparedStatement} and doesn't buffer records in memory. Only used in Table/SQL API.
 */
public final class TableSimpleStatementExecutor implements JdbcBatchStatementExecutor<RowData> {

  private final StatementFactory stmtFactory;
  private final JdbcRowConverter converter;

  private transient BatchWriter st;

  /**
   * Keep in mind object reuse: if it's on then key extractor may be required to return new object.
   */
  public TableSimpleStatementExecutor(StatementFactory stmtFactory, JdbcRowConverter converter) {
    this.stmtFactory = checkNotNull(stmtFactory);
    this.converter = checkNotNull(converter);
  }

  @Override
  public void prepareStatements(Connection connection) throws SQLException {
    st = stmtFactory.createStatement(connection);
  }

  @Override
  public void addToBatch(RowData record) throws Exception {
    converter.toExternal(record, st);
    st.addBatch();
  }

  @Override
  public void executeBatch() throws Exception {
    st.executeBatch();
  }

  @Override
  public void closeStatements() throws Exception {
    if (st != null) {
      st.close();
      st = null;
    }
  }
}
