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

package org.apache.flink.connector.cloudberry;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.cloudberry.common.config.JdbcConnectionOptions;
import org.apache.flink.connector.cloudberry.sink.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Comprehensive integration test demonstrating Flink JDBC Connector with PostgreSQL.
 *
 * <p>This test combines features from both JdbcITCase (DataStream API) and OracleTableSinkITCase
 * (Table API) to provide a complete testing suite for the Cloudberry connector.
 *
 * <p>Test Coverage:
 *
 * <ul>
 *   <li>DataStream API with JdbcSink
 *   <li>Table API with append mode
 *   <li>Table API with upsert mode
 *   <li>Batch mode table sink
 * </ul>
 */
@Testcontainers
@DisplayName("PostgreSQL Integration Test with Flink JDBC Connector")
class IntegrationTest {

  // Start a PostgreSQL container for testing
  // Note: Replace with Cloudberry image when available
  @Container
  static PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
          .withDatabaseName("testdb")
          .withUsername("test")
          .withPassword("test");

  // Test data - similar to JdbcITCase
  private static final BookEntry[] TEST_BOOKS = {
    new BookEntry(1001, "Java Programming", "John Smith", 45.50, 10),
    new BookEntry(1002, "Python Basics", "Jane Doe", 35.00, 15),
    new BookEntry(1003, "SQL Mastery", "Bob Johnson", 52.99, 8),
    new BookEntry(1004, "Flink in Action", "Alice Wang", 60.00, 12),
    new BookEntry(1005, "Data Streaming", "Charlie Brown", 48.75, 20)
  };

  @BeforeAll
  static void setupDatabase() throws SQLException {
    // Create test tables before running tests
    try (Connection conn =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement stmt = conn.createStatement()) {

      // Table for DataStream API test
      stmt.execute(
          "CREATE TABLE books ("
              + "id INT PRIMARY KEY, "
              + "title VARCHAR(200), "
              + "author VARCHAR(100), "
              + "price DECIMAL(10,2), "
              + "quantity INT)");

      // Table for Table API append mode test
      stmt.execute(
          "CREATE TABLE user_orders ("
              + "order_id INT, "
              + "user_name VARCHAR(100), "
              + "amount DECIMAL(10,2), "
              + "order_time TIMESTAMP)");

      // Table for Table API upsert mode test
      stmt.execute(
          "CREATE TABLE user_statistics ("
              + "user_id INT PRIMARY KEY, "
              + "total_orders BIGINT, "
              + "total_amount DECIMAL(18,2), "
              + "last_update TIMESTAMP)");

      // Table for batch mode test
      stmt.execute(
          "CREATE TABLE product_sales (" + "product_name VARCHAR(100), " + "sales_count BIGINT)");

      // Table for COPY protocol test
      stmt.execute(
          "CREATE TABLE bulk_orders ("
              + "order_id INT, "
              + "product_name VARCHAR(100), "
              + "quantity INT, "
              + "total_price DECIMAL(10,2), "
              + "order_date TIMESTAMP)");
    }
  }

  /**
   * Test 1: DataStream API with JdbcSink
   *
   * <p>This test demonstrates how to use Flink's DataStream API with JdbcSink to write data to
   * PostgreSQL. Similar to JdbcITCase.testInsert().
   */
  @Test
  @DisplayName("Test DataStream API - Insert books using JdbcSink")
  void testDataStreamApiInsert() throws Exception {
    // Create Flink streaming environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
    env.setParallelism(1);

    // Create data stream from test data
    env.fromElements(TEST_BOOKS)
        .addSink(
            JdbcSink.sink(
                // SQL insert statement
                "INSERT INTO books (id, title, author, price, quantity) VALUES (?, ?, ?, ?, ?)",
                // Statement builder - binds Java objects to SQL parameters
                (ps, book) -> {
                  ps.setInt(1, book.id);
                  ps.setString(2, book.title);
                  ps.setString(3, book.author);
                  if (book.price == null) {
                    ps.setNull(4, Types.DECIMAL);
                  } else {
                    ps.setDouble(4, book.price);
                  }
                  ps.setInt(5, book.quantity);
                },
                // Cloudberry connection options
                JdbcConnectionOptions.builder()
                    .setJdbcUrl(postgres.getJdbcUrl())
                    .setUsername(postgres.getUsername())
                    .setPassword(postgres.getPassword())
                    .setTable("books")
                    .build()));

    // Execute the Flink job
    env.execute("Insert Books Test");

    // Verify results
    List<BookEntry> results = queryBooks();
    assertThat(results).hasSize(5).containsExactlyInAnyOrder(TEST_BOOKS);
  }

  /**
   * Test 2: Table API with Append Mode
   *
   * <p>This test demonstrates how to use Flink's Table API to write data in append-only mode.
   * Similar to OracleTableSinkITCase.testAppend().
   */
  @Test
  @DisplayName("Test Table API - Append mode with streaming data")
  void testTableApiAppendMode() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // Register sink table with JDBC connector
    tEnv.executeSql(
        "CREATE TABLE order_sink ("
            + "  order_id INT,"
            + "  user_name STRING,"
            + "  amount DECIMAL(10,2),"
            + "  order_time TIMESTAMP(3)"
            + ") WITH ("
            + "  'connector' = 'cloudberry',"
            + "  'url' = '"
            + postgres.getJdbcUrl()
            + "',"
            + "  'username' = '"
            + postgres.getUsername()
            + "',"
            + "  'password' = '"
            + postgres.getPassword()
            + "',"
            + "  'table-name' = 'user_orders'"
            + ")");

    // Insert test data using SQL
    tEnv.executeSql(
            "INSERT INTO order_sink "
                + "SELECT * FROM (VALUES "
                + "(1, 'Alice', 99.99, TIMESTAMP '2024-01-01 10:00:00'), "
                + "(2, 'Bob', 149.50, TIMESTAMP '2024-01-01 11:30:00'), "
                + "(3, 'Charlie', 75.25, TIMESTAMP '2024-01-01 14:15:00')"
                + ") AS orders(order_id, user_name, amount, order_time)")
        .await();

    // Verify results
    List<Row> results = queryOrders();
    assertThat(results).hasSize(3);
    assertThat(results.get(0).getField(1)).isIn("Alice", "Bob", "Charlie");
  }

  /**
   * Test 3: Table API with Upsert Mode
   *
   * <p>This test demonstrates how to use Flink's Table API with a primary key to enable upsert
   * mode. Similar to OracleTableSinkITCase.testUpsert().
   */
  @Test
  @DisplayName("Test Table API - Upsert mode with aggregations")
  void testTableApiUpsertMode() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // Create source table with changelog data
    tEnv.executeSql(
        "CREATE TABLE user_actions ("
            + "  user_id INT,"
            + "  action_type STRING,"
            + "  amount DECIMAL(10,2),"
            + "  action_time TIMESTAMP(3)"
            + ") WITH ("
            + "  'connector' = 'datagen',"
            + "  'number-of-rows' = '0'"
            + ")");

    // Create sink table with primary key (enables upsert mode)
    tEnv.executeSql(
        "CREATE TABLE stats_sink ("
            + "  user_id INT,"
            + "  total_orders BIGINT,"
            + "  total_amount DECIMAL(18,2),"
            + "  last_update TIMESTAMP(3),"
            + "  PRIMARY KEY (user_id) NOT ENFORCED"
            + ") WITH ("
            + "  'connector' = 'cloudberry',"
            + "  'url' = '"
            + postgres.getJdbcUrl()
            + "',"
            + "  'username' = '"
            + postgres.getUsername()
            + "',"
            + "  'password' = '"
            + postgres.getPassword()
            + "',"
            + "  'table-name' = 'user_statistics',"
            + "  'sink.buffer-flush.max-rows' = '2',"
            + "  'sink.buffer-flush.interval' = '0'"
            + ")");

    // Insert aggregated data (will trigger upsert based on primary key)
    tEnv.executeSql(
            "INSERT INTO stats_sink "
                + "SELECT * FROM (VALUES "
                + "(101, 5, 500.00, TIMESTAMP '2024-01-01 12:00:00'), "
                + "(102, 3, 350.50, TIMESTAMP '2024-01-01 12:00:00'), "
                + "(101, 8, 899.99, TIMESTAMP '2024-01-01 13:00:00')"
                + // Update for user 101
                ") AS stats(user_id, total_orders, total_amount, last_update)")
        .await();

    // Verify results - user 101 should have updated values (not two rows)
    List<Row> results = queryUserStatistics();
    assertThat(results).hasSize(2); // Only 2 users, not 3 rows

    // Find user 101's record and verify it was updated
    Row user101 =
        results.stream().filter(r -> ((Integer) r.getField(0)) == 101).findFirst().orElse(null);
    assertThat(user101).isNotNull();
    assertThat(user101.getField(1)).isEqualTo(8L); // Latest total_orders
  }

  /**
   * Test 4: Table API with COPY Protocol (Append Mode)
   *
   * <p>This test demonstrates how to use PostgreSQL COPY protocol for high-performance bulk data
   * loading. COPY protocol is much faster than standard INSERT statements for large batch inserts.
   */
  @Test
  @DisplayName("Test Table API - Append mode with COPY protocol")
  void testTableApiAppendModeWithCopyProtocol() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // Register sink table with JDBC connector and enable COPY protocol
    tEnv.executeSql(
        "CREATE TABLE bulk_orders_sink ("
            + "  order_id INT,"
            + "  product_name STRING,"
            + "  quantity INT,"
            + "  total_price DECIMAL(10,2),"
            + "  order_date TIMESTAMP(3)"
            + ") WITH ("
            + "  'connector' = 'cloudberry',"
            + "  'url' = '"
            + postgres.getJdbcUrl()
            + "',"
            + "  'username' = '"
            + postgres.getUsername()
            + "',"
            + "  'password' = '"
            + postgres.getPassword()
            + "',"
            + "  'table-name' = 'bulk_orders',"
            + "  'sink.use-copy-protocol' = 'true',"
            + // Enable COPY protocol
            "  'sink.buffer-flush.max-rows' = '100',"
            + // Flush after 100 rows
            "  'sink.buffer-flush.interval' = '1s'"
            + // Or flush every 1 second
            ")");

    // Insert test data - simulating bulk data loading
    tEnv.executeSql(
            "INSERT INTO bulk_orders_sink "
                + "SELECT * FROM (VALUES "
                + "(1001, 'Laptop', 2, 2999.98, TIMESTAMP '2024-01-15 09:00:00'), "
                + "(1002, 'Mouse', 5, 149.95, TIMESTAMP '2024-01-15 09:15:00'), "
                + "(1003, 'Keyboard', 3, 299.97, TIMESTAMP '2024-01-15 09:30:00'), "
                + "(1004, 'Monitor', 1, 599.99, TIMESTAMP '2024-01-15 10:00:00'), "
                + "(1005, 'USB Cable', 10, 99.90, TIMESTAMP '2024-01-15 10:30:00')"
                + ") AS orders(order_id, product_name, quantity, total_price, order_date)")
        .await();

    // Verify results
    List<Row> results = queryBulkOrders();
    assertThat(results).hasSize(5);

    // Verify specific order
    Row laptopOrder =
        results.stream().filter(r -> "Laptop".equals(r.getField(1))).findFirst().orElse(null);
    assertThat(laptopOrder).isNotNull();
    assertThat(laptopOrder.getField(0)).isEqualTo(1001); // order_id
    assertThat(laptopOrder.getField(2)).isEqualTo(2); // quantity
  }

  /**
   * Test 5: Batch Mode Table Sink
   *
   * <p>This test demonstrates how to use Flink's Table API in batch mode. Similar to
   * OracleTableSinkITCase.testBatchSink().
   */
  @Test
  @DisplayName("Test Table API - Batch mode execution")
  void testBatchMode() throws Exception {
    // Create table environment in batch mode
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

    // Register sink table
    tEnv.executeSql(
        "CREATE TABLE sales_sink ("
            + "  product_name STRING,"
            + "  sales_count BIGINT"
            + ") WITH ("
            + "  'connector' = 'cloudberry',"
            + "  'url' = '"
            + postgres.getJdbcUrl()
            + "',"
            + "  'username' = '"
            + postgres.getUsername()
            + "',"
            + "  'password' = '"
            + postgres.getPassword()
            + "',"
            + "  'table-name' = 'product_sales',"
            + "  'sink.buffer-flush.max-rows' = '3',"
            + "  'sink.buffer-flush.interval' = '300ms'"
            + ")");

    // Execute batch insert
    tEnv.executeSql(
            "INSERT INTO sales_sink "
                + "SELECT product, COUNT(*) as cnt "
                + "FROM (VALUES "
                + "('Laptop', 1), "
                + "('Mouse', 1), "
                + "('Laptop', 2), "
                + "('Keyboard', 1), "
                + "('Mouse', 2)"
                + ") AS sales(product, sale_id) "
                + "GROUP BY product")
        .await();

    // Verify results
    List<Row> results = queryProductSales();
    assertThat(results).hasSize(3);

    // Verify aggregation results
    Row laptopSales =
        results.stream().filter(r -> "Laptop".equals(r.getField(0))).findFirst().orElse(null);
    assertThat(laptopSales).isNotNull();
    assertThat(laptopSales.getField(1)).isEqualTo(2L);
  }

  // ============ Helper Methods ============

  /** Query books table and return results. */
  private List<BookEntry> queryBooks() throws SQLException {
    List<BookEntry> results = new ArrayList<>();
    try (Connection conn =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM books ORDER BY id")) {
      while (rs.next()) {
        results.add(
            new BookEntry(
                rs.getInt("id"),
                rs.getString("title"),
                rs.getString("author"),
                rs.getDouble("price"),
                rs.getInt("quantity")));
      }
    }
    return results;
  }

  /** Query user_orders table and return results. */
  private List<Row> queryOrders() throws SQLException {
    List<Row> results = new ArrayList<>();
    try (Connection conn =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM user_orders ORDER BY order_id")) {
      while (rs.next()) {
        results.add(
            Row.of(
                rs.getInt("order_id"),
                rs.getString("user_name"),
                rs.getBigDecimal("amount"),
                rs.getTimestamp("order_time")));
      }
    }
    return results;
  }

  /** Query user_statistics table and return results. */
  private List<Row> queryUserStatistics() throws SQLException {
    List<Row> results = new ArrayList<>();
    try (Connection conn =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM user_statistics ORDER BY user_id")) {
      while (rs.next()) {
        results.add(
            Row.of(
                rs.getInt("user_id"),
                rs.getLong("total_orders"),
                rs.getBigDecimal("total_amount"),
                rs.getTimestamp("last_update")));
      }
    }
    return results;
  }

  /** Query product_sales table and return results. */
  private List<Row> queryProductSales() throws SQLException {
    List<Row> results = new ArrayList<>();
    try (Connection conn =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM product_sales ORDER BY product_name")) {
      while (rs.next()) {
        results.add(Row.of(rs.getString("product_name"), rs.getLong("sales_count")));
      }
    }
    return results;
  }

  /** Query bulk_orders table and return results. */
  private List<Row> queryBulkOrders() throws SQLException {
    List<Row> results = new ArrayList<>();
    try (Connection conn =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM bulk_orders ORDER BY order_id")) {
      while (rs.next()) {
        results.add(
            Row.of(
                rs.getInt("order_id"),
                rs.getString("product_name"),
                rs.getInt("quantity"),
                rs.getBigDecimal("total_price"),
                rs.getTimestamp("order_date")));
      }
    }
    return results;
  }

  // ============ Test Data Classes ============

  /** Book entry for testing - similar to TestEntry in JdbcTestFixture. */
  static class BookEntry implements Serializable {
    final Integer id;
    final String title;
    final String author;
    final Double price;
    final Integer quantity;

    BookEntry(Integer id, String title, String author, Double price, Integer quantity) {
      this.id = id;
      this.title = title;
      this.author = author;
      this.price = price;
      this.quantity = quantity;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BookEntry that = (BookEntry) o;
      return java.util.Objects.equals(id, that.id)
          && java.util.Objects.equals(title, that.title)
          && java.util.Objects.equals(author, that.author)
          && java.util.Objects.equals(price, that.price)
          && java.util.Objects.equals(quantity, that.quantity);
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(id, title, author, price, quantity);
    }

    @Override
    public String toString() {
      return "BookEntry{"
          + "id="
          + id
          + ", title='"
          + title
          + '\''
          + ", author='"
          + author
          + '\''
          + ", price="
          + price
          + ", quantity="
          + quantity
          + '}';
    }
  }
}
