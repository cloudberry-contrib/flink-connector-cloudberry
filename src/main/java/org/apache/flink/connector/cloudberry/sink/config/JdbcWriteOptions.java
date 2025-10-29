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

package org.apache.flink.connector.cloudberry.sink.config;

import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.DELIVERY_GUARANTEE;
import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.SINK_RETRY_INTERVAL;
import static org.apache.flink.connector.cloudberry.table.JdbcConnectorOptions.SINK_USE_COPY_PROTOCOL;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Objects;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;

/**
 * Configurations for CloudberrySink to control write operations. All the options listed here can be
 * configured by {@link CloudberryWriteOptionsBuilder}.
 *
 * <p>These options control batching behavior, retry logic, and delivery guarantees for writing data
 * to Cloudberry database.
 */
@PublicEvolving
public final class JdbcWriteOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int batchSize;
  private final long batchIntervalMs;
  private final int maxRetries;
  private final long retryIntervalMs;
  // Store delivery guarantee as String to avoid serialization issues
  // The DeliveryGuarantee enum may contain non-serializable Description objects
  private final String deliveryGuarantee;
  private final boolean useCopyProtocol;

  private JdbcWriteOptions(
      int batchSize,
      long batchIntervalMs,
      int maxRetries,
      long retryIntervalMs,
      String deliveryGuarantee,
      boolean useCopyProtocol) {
    this.batchSize = batchSize;
    this.batchIntervalMs = batchIntervalMs;
    this.maxRetries = maxRetries;
    this.retryIntervalMs = retryIntervalMs;
    this.deliveryGuarantee = deliveryGuarantee;
    this.useCopyProtocol = useCopyProtocol;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public long getBatchIntervalMs() {
    return batchIntervalMs;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public long getRetryIntervalMs() {
    return retryIntervalMs;
  }

  /**
   * Gets the delivery guarantee as a DeliveryGuarantee enum.
   *
   * @return the delivery guarantee
   */
  public DeliveryGuarantee getDeliveryGuarantee() {
    // Convert String back to enum when needed
    return DeliveryGuarantee.valueOf(deliveryGuarantee);
  }

  /**
   * Gets the delivery guarantee as a String (for internal use).
   *
   * @return the delivery guarantee string
   */
  public String getDeliveryGuaranteeString() {
    return deliveryGuarantee;
  }

  public boolean isUseCopyProtocol() {
    return useCopyProtocol;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JdbcWriteOptions that = (JdbcWriteOptions) o;
    return batchSize == that.batchSize
        && batchIntervalMs == that.batchIntervalMs
        && maxRetries == that.maxRetries
        && retryIntervalMs == that.retryIntervalMs
        && Objects.equals(deliveryGuarantee, that.deliveryGuarantee)
        && useCopyProtocol == that.useCopyProtocol;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        batchSize,
        batchIntervalMs,
        maxRetries,
        retryIntervalMs,
        deliveryGuarantee,
        useCopyProtocol);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static JdbcWriteOptions defaults() {
    return builder().build();
  }

  /** Builder for {@link CloudberryWriteOptions}. */
  @PublicEvolving
  public static class Builder {
    private int batchSize = BUFFER_FLUSH_MAX_ROWS.defaultValue();
    private long batchIntervalMs = BUFFER_FLUSH_INTERVAL.defaultValue().toMillis();
    private int maxRetries = SINK_MAX_RETRIES.defaultValue();
    private long retryIntervalMs = SINK_RETRY_INTERVAL.defaultValue().toMillis();
    // Store as String to avoid serialization issues with ConfigOption
    private String deliveryGuarantee = DELIVERY_GUARANTEE.defaultValue().name();
    private boolean useCopyProtocol = SINK_USE_COPY_PROTOCOL.defaultValue();

    private Builder() {}

    /**
     * Sets the maximum number of records to buffer for each batch request. You can pass -1 to
     * disable batching.
     *
     * @param batchSize the maximum number of records to buffer for each batch request.
     * @return this builder
     */
    public Builder setBatchSize(int batchSize) {
      checkArgument(
          batchSize == -1 || batchSize > 0,
          "Batch size must be larger than 0 or -1 to disable batching.");
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Sets the batch flush interval, in milliseconds. You can pass -1 to disable it.
     *
     * <p>When this interval is reached, the batch will be flushed even if the batch size has not
     * been reached.
     *
     * @param batchIntervalMs the batch flush interval, in milliseconds.
     * @return this builder
     */
    public Builder setBatchIntervalMs(long batchIntervalMs) {
      checkArgument(
          batchIntervalMs == -1 || batchIntervalMs >= 0,
          "The batch flush interval (in milliseconds) must be larger than "
              + "or equal to 0, or -1 to disable.");
      this.batchIntervalMs = batchIntervalMs;
      return this;
    }

    /**
     * Sets the max retry times if writing records failed.
     *
     * <p>When a write operation fails, the connector will retry up to this number of times before
     * giving up.
     *
     * @param maxRetries the max retry times.
     * @return this builder
     */
    public Builder setMaxRetries(int maxRetries) {
      checkArgument(maxRetries >= 0, "The sink max retry times must be larger than or equal to 0.");
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * Sets the retry interval if writing records to database failed.
     *
     * <p>This is the base interval; actual retry intervals may increase exponentially with each
     * retry attempt.
     *
     * @param retryIntervalMs the retry time interval, in milliseconds.
     * @return this builder
     */
    public Builder setRetryIntervalMs(long retryIntervalMs) {
      checkArgument(
          retryIntervalMs > 0, "The retry interval (in milliseconds) must be larger than 0.");
      this.retryIntervalMs = retryIntervalMs;
      return this;
    }

    /**
     * Sets the wanted {@link DeliveryGuarantee}. The default delivery guarantee is {@link
     * DeliveryGuarantee#AT_LEAST_ONCE}.
     *
     * <p>Note: EXACTLY_ONCE is not supported for Cloudberry sink.
     *
     * @param deliveryGuarantee which describes the record emission behaviour
     * @return this builder
     */
    public Builder setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
      checkArgument(
          deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE,
          "Cloudberry sink does not support the EXACTLY_ONCE guarantee.");
      checkNotNull(deliveryGuarantee);
      // Store as String to avoid serialization issues
      this.deliveryGuarantee = deliveryGuarantee.name();
      return this;
    }

    /**
     * Sets whether to use PostgreSQL COPY protocol for bulk data loading.
     *
     * <p>When enabled, the sink will use COPY protocol which provides better performance for large
     * batch inserts compared to standard INSERT statements. This is particularly useful when
     * writing large volumes of data to Cloudberry database.
     *
     * <p>Note: COPY protocol may have different transactional semantics and error handling behavior
     * compared to standard INSERT statements.
     *
     * @param useCopyProtocol true to enable COPY protocol, false to use standard INSERT
     * @return this builder
     */
    public Builder setUseCopyProtocol(boolean useCopyProtocol) {
      this.useCopyProtocol = useCopyProtocol;
      return this;
    }

    /**
     * Build the {@link CloudberryWriteOptions}.
     *
     * @return a CloudberryWriteOptions with the settings made for this builder.
     */
    public JdbcWriteOptions build() {
      return new JdbcWriteOptions(
          batchSize,
          batchIntervalMs,
          maxRetries,
          retryIntervalMs,
          deliveryGuarantee,
          useCopyProtocol);
    }
  }
}
