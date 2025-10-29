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

package org.apache.flink.connector.cloudberry.common.config;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

/**
 * Connection options for Cloudberry database.
 *
 * <p>This class holds all the necessary information to connect to a Cloudberry database, including
 * JDBC URL (with database name), username, password, and table name.
 */
@PublicEvolving
public final class JdbcConnectionOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  // JDBC connection URL for Cloudberry database (includes database name)
  private final String jdbcUrl;

  // Database username
  private final String username;

  // Database password
  private final String password;

  // Target table name
  private final String table;

  private final int connectionCheckTimeoutSeconds;

  private @Nullable Integer parallelism;

  private JdbcConnectionOptions(
      String jdbcUrl,
      String username,
      String password,
      String table,
      int connectionCheckTimeoutSeconds,
      Integer parallelism) {
    Preconditions.checkArgument(
        connectionCheckTimeoutSeconds > 0,
        "Connection check timeout seconds shouldn't be smaller than 1");
    this.jdbcUrl = checkNotNull(jdbcUrl, "JDBC URL must not be null");
    this.username = username;
    this.password = password;
    this.table = checkNotNull(table, "Table must not be null");
    this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
    this.parallelism = parallelism;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getTable() {
    return table;
  }

  public int getConnectionCheckTimeoutSeconds() {
    return connectionCheckTimeoutSeconds;
  }

  public Integer getParallelism() {
    return parallelism;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JdbcConnectionOptions that = (JdbcConnectionOptions) o;
    return Objects.equals(jdbcUrl, that.jdbcUrl)
        && Objects.equals(username, that.username)
        && Objects.equals(password, that.password)
        && Objects.equals(table, that.table)
        && connectionCheckTimeoutSeconds == that.connectionCheckTimeoutSeconds
        && Objects.equals(parallelism, that.parallelism);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        jdbcUrl, username, password, table, connectionCheckTimeoutSeconds, parallelism);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link CloudberryConnectionOptions}. */
  @PublicEvolving
  public static class Builder {
    private String jdbcUrl;
    private String username;
    private String password;
    private String table;
    private int connectionCheckTimeoutSeconds = 60;
    private Integer parallelism = null;

    private Builder() {}

    /**
     * Sets the JDBC URL for the Cloudberry database.
     *
     * <p>The JDBC URL should include the database name.
     *
     * <p>Example: jdbc:postgresql://localhost:5432/mydb
     *
     * @param jdbcUrl the JDBC connection URL (including database name)
     * @return this builder
     */
    public Builder setJdbcUrl(String jdbcUrl) {
      this.jdbcUrl = jdbcUrl;
      return this;
    }

    /**
     * Sets the username for database authentication.
     *
     * @param username the database username
     * @return this builder
     */
    public Builder setUsername(String username) {
      this.username = username;
      return this;
    }

    /**
     * Sets the password for database authentication.
     *
     * @param password the database password
     * @return this builder
     */
    public Builder setPassword(String password) {
      this.password = password;
      return this;
    }

    /**
     * Sets the target table name.
     *
     * @param table the table name
     * @return this builder
     */
    public Builder setTable(String table) {
      this.table = table;
      return this;
    }

    /**
     * Set the maximum timeout between retries, default is 60 seconds.
     *
     * @param connectionCheckTimeoutSeconds the timeout seconds, shouldn't smaller than 1 second.
     */
    public Builder withConnectionCheckTimeoutSeconds(int connectionCheckTimeoutSeconds) {
      this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
      return this;
    }

    public Builder setParallelism(Integer parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    /**
     * Build the {@link CloudberryConnectionOptions}.
     *
     * @return a CloudberryConnectionOptions with the settings made for this builder.
     */
    public JdbcConnectionOptions build() {
      return new JdbcConnectionOptions(
          jdbcUrl, username, password, table, connectionCheckTimeoutSeconds, parallelism);
    }
  }
}
