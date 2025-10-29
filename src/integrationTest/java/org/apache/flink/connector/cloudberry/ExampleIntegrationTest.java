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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Example integration test demonstrating Testcontainers usage with PostgreSQL.
 *
 * <p>This can be used as a template for Cloudberry integration tests. Replace the PostgreSQL image
 * with Cloudberry image when available.
 */
@Testcontainers
@DisplayName("Example Integration Test with Testcontainers")
class ExampleIntegrationTest {

  // Start a PostgreSQL container for testing
  // Note: Replace with Cloudberry image when available
  @Container
  static PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
          .withDatabaseName("testdb")
          .withUsername("test")
          .withPassword("test");

  @Test
  @DisplayName("Should connect to PostgreSQL container and execute query")
  void testDatabaseConnection() throws Exception {
    // Get connection details from the container
    String jdbcUrl = postgres.getJdbcUrl();
    String username = postgres.getUsername();
    String password = postgres.getPassword();

    assertThat(jdbcUrl).isNotNull();
    assertThat(postgres.isRunning()).isTrue();

    // Test database connection
    try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
        Statement stmt = conn.createStatement()) {

      // Create a test table
      stmt.execute("CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100))");
      stmt.execute("INSERT INTO test_table VALUES (1, 'Cloudberry')");

      // Query the data
      ResultSet rs = stmt.executeQuery("SELECT name FROM test_table WHERE id = 1");

      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("name")).isEqualTo("Cloudberry");
    }
  }

  @Test
  @DisplayName("Should verify container provides correct JDBC URL format")
  void testJdbcUrlFormat() {
    String jdbcUrl = postgres.getJdbcUrl();

    assertThat(jdbcUrl).startsWith("jdbc:postgresql://").contains("testdb");
  }
}
