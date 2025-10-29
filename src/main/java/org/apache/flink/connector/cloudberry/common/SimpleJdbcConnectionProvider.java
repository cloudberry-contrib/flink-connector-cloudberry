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

package org.apache.flink.connector.cloudberry.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.cloudberry.common.config.JdbcConnectionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Simple JDBC connection provider. */
@NotThreadSafe
@PublicEvolving
public class SimpleJdbcConnectionProvider implements JdbcConnectionProvider {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleJdbcConnectionProvider.class);

  private static final long serialVersionUID = 1L;

  private final JdbcConnectionOptions jdbcOptions;

  private transient Connection connection;

  public SimpleJdbcConnectionProvider(JdbcConnectionOptions jdbcOptions) {
    this.jdbcOptions = jdbcOptions;
  }

  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public boolean isConnectionValid() throws SQLException {
    return connection != null
        && !connection.isClosed()
        && connection.isValid(jdbcOptions.getConnectionCheckTimeoutSeconds());
  }

  @Override
  public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
    if (isConnectionValid()) {
      return connection;
    }

    Properties props = new Properties();
    if (Objects.nonNull(jdbcOptions.getUsername())) {
      props.setProperty("user", jdbcOptions.getUsername());
    }
    if (Objects.nonNull(jdbcOptions.getPassword())) {
      props.setProperty("password", jdbcOptions.getPassword());
    }

    connection = DriverManager.getConnection(jdbcOptions.getJdbcUrl(), props);
    return connection;
  }

  @Override
  public void closeConnection() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        LOG.warn("JDBC connection close failed.", e);
      } finally {
        connection = null;
      }
    }
  }

  @Override
  public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
    closeConnection();
    return getOrEstablishConnection();
  }
}
