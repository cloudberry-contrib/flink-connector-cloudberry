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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.cloudberry.common.JdbcStatementBuilder;

@Internal
class SimpleBatchStatementExecutor<T> implements JdbcBatchStatementExecutor<T> {
  private final String sql;
  private final JdbcStatementBuilder<T> parameterSetter;
  private final List<T> batch;

  private transient PreparedStatement st;

  SimpleBatchStatementExecutor(String sql, JdbcStatementBuilder<T> statementBuilder) {
    this.sql = sql;
    this.parameterSetter = statementBuilder;
    this.batch = new ArrayList<>();
  }

  @Override
  public void prepareStatements(Connection connection) throws SQLException {
    this.st = connection.prepareStatement(sql);
  }

  @Override
  public void addToBatch(T record) throws Exception {
    batch.add(record);
  }

  @Override
  public void executeBatch() throws Exception {
    if (!batch.isEmpty()) {
      for (T r : batch) {
        parameterSetter.accept(st, r);
        st.addBatch();
      }
      st.executeBatch();
      batch.clear();
    }
  }

  @Override
  public void closeStatements() throws Exception {
    if (st != null) {
      st.close();
      st = null;
    }
  }
}
