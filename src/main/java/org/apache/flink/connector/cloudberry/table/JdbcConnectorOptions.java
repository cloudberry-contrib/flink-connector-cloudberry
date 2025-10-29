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

package org.apache.flink.connector.cloudberry.table;

import java.time.Duration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;

/**
 * Base options for the Cloudberry connector. Needs to be public so that the {@link
 * org.apache.flink.table.api.TableDescriptor} can access it.
 */
@PublicEvolving
public class JdbcConnectorOptions {

  private JdbcConnectorOptions() {}

  public static final ConfigOption<String> URL =
      ConfigOptions.key("url")
          .stringType()
          .noDefaultValue()
          .withDescription("Specifies the JDBC connection URL for Cloudberry database.");

  public static final ConfigOption<String> USERNAME =
      ConfigOptions.key("username")
          .stringType()
          .noDefaultValue()
          .withDescription("Specifies the username for database authentication.");

  public static final ConfigOption<String> PASSWORD =
      ConfigOptions.key("password")
          .stringType()
          .noDefaultValue()
          .withDescription("Specifies the password for database authentication.");

  public static final ConfigOption<String> TABLE_NAME =
      ConfigOptions.key("table-name")
          .stringType()
          .noDefaultValue()
          .withDescription("Specifies the table name to read or write.");

  public static final ConfigOption<Integer> BUFFER_FLUSH_MAX_ROWS =
      ConfigOptions.key("sink.buffer-flush.max-rows")
          .intType()
          .defaultValue(1000)
          .withDescription("Specifies the maximum number of buffered rows per batch request.");

  public static final ConfigOption<Duration> BUFFER_FLUSH_INTERVAL =
      ConfigOptions.key("sink.buffer-flush.interval")
          .durationType()
          .defaultValue(Duration.ofSeconds(5))
          .withDescription("Specifies the batch flush interval.");

  public static final ConfigOption<DeliveryGuarantee> DELIVERY_GUARANTEE =
      ConfigOptions.key("sink.delivery-guarantee")
          .enumType(DeliveryGuarantee.class)
          .defaultValue(DeliveryGuarantee.AT_LEAST_ONCE)
          .withDescription(
              "Optional delivery guarantee when committing. "
                  + "The exactly-once guarantee is not supported yet.");

  public static final ConfigOption<Integer> SINK_MAX_RETRIES =
      ConfigOptions.key("sink.max-retries")
          .intType()
          .defaultValue(3)
          .withDescription("Specifies the max retry times if writing records to database failed.");

  public static final ConfigOption<Duration> SINK_RETRY_INTERVAL =
      ConfigOptions.key("sink.retry.interval")
          .durationType()
          .defaultValue(Duration.ofSeconds(1))
          .withDescription(
              "Specifies the retry time interval if writing records to database failed.");

  public static final ConfigOption<Boolean> SINK_USE_COPY_PROTOCOL =
      ConfigOptions.key("sink.use-copy-protocol")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Specifies whether to use PostgreSQL COPY protocol for bulk data loading. "
                  + "When enabled, provides better performance for large batch inserts. "
                  + "Default is false (uses standard INSERT statements).");
}
