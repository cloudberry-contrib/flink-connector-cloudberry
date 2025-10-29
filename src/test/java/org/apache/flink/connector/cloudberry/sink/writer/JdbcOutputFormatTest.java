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

import static org.apache.flink.connector.cloudberry.JdbcTestFixture.FIELD_DATA_TYPES;
import static org.apache.flink.connector.cloudberry.JdbcTestFixture.OUTPUT_TABLE;
import static org.apache.flink.connector.cloudberry.JdbcTestFixture.ROW_DATA_TYPE_INFO;
import static org.apache.flink.connector.cloudberry.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.cloudberry.JdbcTestFixture.TestEntry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.cloudberry.JdbcDataTestBase;
import org.apache.flink.connector.cloudberry.common.JdbcDmlOptions;
import org.apache.flink.connector.cloudberry.common.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.cloudberry.common.config.JdbcConnectionOptions;
import org.apache.flink.connector.cloudberry.sink.config.JdbcWriteOptions;
import org.apache.flink.connector.cloudberry.sink.writer.executor.JdbcBatchStatementExecutor;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/** Tests for the {@link org.apache.flink.connector.cloudberry.sink.writer.JdbcOutputFormat}. */
public class JdbcOutputFormatTest extends JdbcDataTestBase {

  private JdbcOutputFormat format;
  private String[] fieldNames;
  private String[] keyFields;

  @Before
  public void setup() {
    fieldNames = new String[] {"id", "title", "author", "price", "qty"};
    keyFields = new String[] {"id"};
  }

  @Test
  public void testUpsertFormatCloseBeforeOpen() throws Exception {
    JdbcConnectionOptions options =
        JdbcConnectionOptions.builder()
            .setJdbcUrl(getDbMetadata().getUrl())
            .setTable(OUTPUT_TABLE)
            .build();
    JdbcDmlOptions dmlOptions =
        JdbcDmlOptions.builder()
            .withTableName(options.getTable())
            .withFieldNames(fieldNames)
            .withKeyFields(keyFields)
            .build();
    // Use Builder pattern to create JdbcOutputFormat with type information
    format =
        new JdbcOutputFormatBuilder()
            .setJdbcOptions(options)
            .setJdbcDmlOptions(dmlOptions)
            .setJdbcWriteOptions(JdbcWriteOptions.defaults())
            .setRowDataTypeInfo(ROW_DATA_TYPE_INFO)
            .setFieldDataTypes(FIELD_DATA_TYPES)
            .build();
    // FLINK-17544: There should be no NPE thrown from this method
    format.close();
  }

  /**
   * Test that the executors are updated when {@link
   * org.apache.flink.connector.cloudberry.sink.writer.JdbcOutputFormat#flush()} fails and
   * reconnection happens.
   */
  @Test
  @SuppressWarnings("unchecked") // Raw type usage is acceptable in test code
  public void testExecutorUpdatedOnReconnect() throws Exception {
    // Track test execution state
    boolean[] exceptionThrown = {false};
    boolean[] executorPreparedAfterReconnect = {false};
    boolean[] executedSuccessfully = {false};

    // Create a custom connection provider that simulates connection failure
    SimpleJdbcConnectionProvider connectionProvider =
        new SimpleJdbcConnectionProvider(
            JdbcConnectionOptions.builder()
                .setJdbcUrl(getDbMetadata().getUrl())
                .setTable(OUTPUT_TABLE)
                .build()) {
          @Override
          public boolean isConnectionValid() throws SQLException {
            // Return false after first exception to trigger reconnect
            return !exceptionThrown[0];
          }
        };

    // Create custom write options with retry enabled
    JdbcWriteOptions writeOptions =
        JdbcWriteOptions.builder().setBatchSize(100).setMaxRetries(1).setBatchIntervalMs(0).build();

    // Create a custom statement executor factory that tracks prepare and execute calls
    JdbcOutputFormat.StatementExecutorFactory<JdbcBatchStatementExecutor<Row>> executorFactory =
        ctx ->
            new JdbcBatchStatementExecutor<Row>() {
              @Override
              public void prepareStatements(Connection connection) throws SQLException {
                // Track that prepare was called after exception (during reconnect)
                if (exceptionThrown[0]) {
                  executorPreparedAfterReconnect[0] = true;
                }
              }

              @Override
              public void addToBatch(Row record) throws SQLException {
                // Do nothing - we're just testing reconnect logic
              }

              @Override
              public void executeBatch() throws SQLException {
                // Throw exception on first attempt to trigger reconnect
                if (!exceptionThrown[0]) {
                  exceptionThrown[0] = true;
                  throw new SQLException("Simulated batch execution failure");
                }
                // Succeed on retry after reconnect
                executedSuccessfully[0] = true;
              }

              @Override
              public void closeStatements() throws SQLException {
                // Clean up resources
              }
            };

    // Create JdbcOutputFormat with the custom factory
    format =
        new JdbcOutputFormat<>(
            connectionProvider,
            writeOptions,
            executorFactory,
            JdbcOutputFormat.RecordExtractor.identity());

    // Set up mocked runtime context
    RuntimeContext context = Mockito.mock(RuntimeContext.class);
    ExecutionConfig config = Mockito.mock(ExecutionConfig.class);
    doReturn(config).when(context).getExecutionConfig();
    doReturn(true).when(config).isObjectReuseEnabled();
    format.setRuntimeContext(context);

    // Open format and write a record
    format.open(0, 1);
    format.writeRecord(toRow(TEST_DATA[0]));

    // Flush should trigger exception, reconnect, and successful retry
    format.flush();

    // Verify that reconnect happened and executor was prepared again
    assertThat(exceptionThrown[0])
        .as("Exception should have been thrown on first attempt")
        .isTrue();
    assertThat(executorPreparedAfterReconnect[0])
        .as("Executor should be prepared after reconnect")
        .isTrue();
    assertThat(executedSuccessfully[0])
        .as("Batch should execute successfully after reconnect")
        .isTrue();
  }

  @Test
  @SuppressWarnings("unchecked") // Raw type usage is acceptable in test code
  public void testJdbcOutputFormat() throws Exception {
    JdbcConnectionOptions options =
        JdbcConnectionOptions.builder()
            .setJdbcUrl(getDbMetadata().getUrl())
            .setTable(OUTPUT_TABLE)
            .build();
    // Use append-only mode (no key fields) - Derby doesn't support ON CONFLICT
    JdbcDmlOptions dmlOptions =
        JdbcDmlOptions.builder()
            .withTableName(options.getTable())
            .withFieldNames(fieldNames)
            .build();
    format =
        new JdbcOutputFormatBuilder()
            .setJdbcOptions(options)
            .setJdbcDmlOptions(dmlOptions)
            .setJdbcWriteOptions(JdbcWriteOptions.defaults())
            .setRowDataTypeInfo(ROW_DATA_TYPE_INFO)
            .setFieldDataTypes(FIELD_DATA_TYPES)
            .build();
    RuntimeContext context = Mockito.mock(RuntimeContext.class);
    ExecutionConfig config = Mockito.mock(ExecutionConfig.class);
    doReturn(config).when(context).getExecutionConfig();
    doReturn(true).when(config).isObjectReuseEnabled();
    format.setRuntimeContext(context);
    format.open(0, 1);

    // Test 1: Insert first batch of TEST_DATA (using RowData)
    for (TestEntry entry : TEST_DATA) {
      format.writeRecord(
          buildGenericData(entry.id, entry.title, entry.author, entry.price, entry.qty));
    }
    format.flush();
    check(Arrays.stream(TEST_DATA).map(JdbcDataTestBase::toRow).toArray(Row[]::new));

    // Test 2: Append second batch with different IDs (avoid primary key conflict)
    // In append-only mode, we simply append more rows
    for (int i = 0; i < TEST_DATA.length; i++) {
      TestEntry entry = TEST_DATA[i];
      // Use different IDs to avoid primary key constraint violation
      format.writeRecord(
          buildGenericData(
              entry.id + 1000, // Offset ID to avoid conflicts
              entry.title + "_copy",
              entry.author,
              entry.price,
              entry.qty));
    }
    format.flush();

    // Verify: should have TEST_DATA.length * 2 rows (original + copies)
    assertThat(countRows()).isEqualTo(TEST_DATA.length * 2);
  }

  private int countRows() throws SQLException {
    try (Connection dbConn = DriverManager.getConnection(getDbMetadata().getUrl());
        Statement stmt = dbConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + OUTPUT_TABLE)) {
      if (rs.next()) {
        return rs.getInt(1);
      }
      return 0;
    }
  }

  private void check(Row[] rows) throws SQLException {
    check(rows, getDbMetadata().getUrl(), OUTPUT_TABLE, fieldNames);
  }

  public static void check(Row[] rows, String url, String table, String[] fields)
      throws SQLException {
    try (Connection dbConn = DriverManager.getConnection(url);
        PreparedStatement statement = dbConn.prepareStatement("select * from " + table);
        ResultSet resultSet = statement.executeQuery()) {
      List<String> results = new ArrayList<>();
      while (resultSet.next()) {
        Row row = new Row(fields.length);
        for (int i = 0; i < fields.length; i++) {
          row.setField(i, resultSet.getObject(fields[i]));
        }
        results.add(row.toString());
      }
      String[] sortedExpect = Arrays.stream(rows).map(Row::toString).toArray(String[]::new);
      String[] sortedResult = results.toArray(new String[0]);
      Arrays.sort(sortedExpect);
      Arrays.sort(sortedResult);
      assertThat(sortedResult).isEqualTo(sortedExpect);
    }
  }

  @After
  public void clearOutputTable() throws Exception {
    if (format != null) {
      format.close();
    }
    format = null;
    try (Connection conn = DriverManager.getConnection(getDbMetadata().getUrl());
        Statement stat = conn.createStatement()) {
      stat.execute("DELETE FROM " + OUTPUT_TABLE);

      stat.close();
      conn.close();
    }
  }
}
