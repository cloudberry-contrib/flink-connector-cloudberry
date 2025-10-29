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

import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.DELIVERY_GUARANTEE;
import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.PASSWORD;
import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.SINK_RETRY_INTERVAL;
import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.SINK_USE_COPY_PROTOCOL;
import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.TABLE_NAME;
import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.URL;
import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.USERNAME;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.cloudberry.common.JdbcDmlOptions;
import org.apache.flink.connector.cloudberry.common.JdbcQueryBuilder;
import org.apache.flink.connector.cloudberry.common.config.JdbcConnectionOptions;
import org.apache.flink.connector.cloudberry.sink.config.JdbcWriteOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

/**
 * Factory for creating configured instances of {@link JdbcDynamicTableSource} and {@link
 * JdbcDynamicTableSink}.
 */
@Internal
public class JdbcDynamicTableFactory implements DynamicTableSinkFactory {

  public static final String IDENTIFIER = "cloudberry";

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    final FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(this, context);
    final ReadableConfig config = helper.getOptions();

    helper.validate();
    validateConfigOptions(config);
    validateDataType(context.getPhysicalRowDataType(), config.get(URL));
    JdbcConnectionOptions jdbcOptions = getJdbcOptions(config);

    return new JdbcDynamicTableSink(
        jdbcOptions,
        getJdbcWriteOptions(config),
        getJdbcDmlOptions(
            jdbcOptions, context.getPhysicalRowDataType(), context.getPrimaryKeyIndexes()),
        context.getPhysicalRowDataType());
  }

  private static void validateDataType(DataType dataType, String url) {
    final JdbcQueryBuilder queryBuilder = JdbcQueryBuilder.getInstance();
    queryBuilder.validate((RowType) dataType.getLogicalType());
  }

  private JdbcConnectionOptions getJdbcOptions(ReadableConfig readableConfig) {
    final String url = readableConfig.get(URL);
    final JdbcConnectionOptions.Builder builder =
        JdbcConnectionOptions.builder()
            .setJdbcUrl(url)
            .setTable(readableConfig.get(TABLE_NAME))
            .setParallelism(readableConfig.getOptional(SINK_PARALLELISM).orElse(null));

    readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
    readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
    return builder.build();
  }

  private JdbcWriteOptions getJdbcWriteOptions(ReadableConfig config) {
    final JdbcWriteOptions.Builder builder = JdbcWriteOptions.builder();
    builder.setBatchSize(config.get(BUFFER_FLUSH_MAX_ROWS));
    builder.setBatchIntervalMs(config.get(BUFFER_FLUSH_INTERVAL).toMillis());
    builder.setMaxRetries(config.get(SINK_MAX_RETRIES));
    builder.setRetryIntervalMs(config.get(SINK_RETRY_INTERVAL).toMillis());
    builder.setDeliveryGuarantee(config.get(DELIVERY_GUARANTEE));
    builder.setUseCopyProtocol(config.get(SINK_USE_COPY_PROTOCOL));
    return builder.build();
  }

  private JdbcDmlOptions getJdbcDmlOptions(
      JdbcConnectionOptions jdbcOptions, DataType dataType, int[] primaryKeyIndexes) {

    String[] keyFields =
        Arrays.stream(primaryKeyIndexes)
            .mapToObj(i -> DataType.getFieldNames(dataType).get(i))
            .toArray(String[]::new);

    return JdbcDmlOptions.builder()
        .withTableName(jdbcOptions.getTable())
        .withFieldNames(DataType.getFieldNames(dataType).toArray(new String[0]))
        .withKeyFields(keyFields.length > 0 ? keyFields : null)
        .build();
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> requiredOptions = new HashSet<>();
    requiredOptions.add(URL);
    requiredOptions.add(TABLE_NAME);
    return requiredOptions;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> optionalOptions = new HashSet<>();
    optionalOptions.add(USERNAME);
    optionalOptions.add(PASSWORD);
    optionalOptions.add(SINK_MAX_RETRIES);
    optionalOptions.add(SINK_PARALLELISM);
    optionalOptions.add(BUFFER_FLUSH_MAX_ROWS);
    optionalOptions.add(BUFFER_FLUSH_INTERVAL);
    optionalOptions.add(DELIVERY_GUARANTEE);
    optionalOptions.add(SINK_USE_COPY_PROTOCOL);
    return optionalOptions;
  }

  @Override
  public Set<ConfigOption<?>> forwardOptions() {
    return Stream.of(
            URL,
            TABLE_NAME,
            USERNAME,
            PASSWORD,
            SINK_MAX_RETRIES,
            BUFFER_FLUSH_MAX_ROWS,
            BUFFER_FLUSH_INTERVAL,
            DELIVERY_GUARANTEE,
            SINK_RETRY_INTERVAL,
            SINK_USE_COPY_PROTOCOL)
        .collect(Collectors.toSet());
  }

  private void validateConfigOptions(ReadableConfig config) {
    checkAllOrNone(config, new ConfigOption[] {USERNAME, PASSWORD});

    if (config.get(SINK_MAX_RETRIES) < 0) {
      throw new IllegalArgumentException(
          String.format(
              "The value of '%s' option shouldn't be negative, but is %s.",
              SINK_MAX_RETRIES.key(), config.get(SINK_MAX_RETRIES)));
    }
  }

  private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
    int presentCount = 0;
    for (ConfigOption<?> configOption : configOptions) {
      if (config.getOptional(configOption).isPresent()) {
        presentCount++;
      }
    }
    String[] propertyNames =
        Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
    Preconditions.checkArgument(
        configOptions.length == presentCount || presentCount == 0,
        "Either all or none of the following options should be provided:\n"
            + String.join("\n", propertyNames));
  }
}
