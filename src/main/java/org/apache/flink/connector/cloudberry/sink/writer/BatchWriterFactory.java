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
import java.sql.Connection;
import java.sql.SQLException;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public final class BatchWriterFactory implements Serializable {
  public static BatchWriter createBatchWriter(
      Connection connection, String tableOrSql, String[] fieldNames, boolean useCopyProtocol)
      throws SQLException {

    checkNotNull(connection, "connection must not be null");
    checkNotNull(tableOrSql, "tableOrSql parameter must not be null");
    checkNotNull(fieldNames, "fieldNames must not be null");

    if (useCopyProtocol) {
      String tableName = tableOrSql;
      // Unwrap to get PostgreSQL specific connection
      BaseConnection pgConnection = connection.unwrap(BaseConnection.class);
      CopyManager copyManager = new CopyManager(pgConnection);

      // Build the COPY SQL command inside the factory
      StringBuilder sb = new StringBuilder();
      sb.append("COPY ").append(tableName).append(" (");
      for (int i = 0; i < fieldNames.length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(fieldNames[i]);
      }
      sb.append(") FROM STDIN WITH (FORMAT CSV)");
      String copySQL = sb.toString();

      CopyIn copyInOperation = copyManager.copyIn(copySQL);
      return new CopyProtocolBatchWriter(copyInOperation, fieldNames);
    } else {
      String sql = tableOrSql;
      return new NamedFieldBatchWriter(connection.prepareStatement(sql), fieldNames);
    }
  }

  // Private constructor to prevent instantiation
  private BatchWriterFactory() {
    throw new AssertionError("Factory class should not be instantiated");
  }
}
