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

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.postgresql.copy.CopyIn;

public class CopyProtocolBatchWriter implements BatchWriter {
  private final CopyIn copyInOperation;
  private final String[] currentRow;
  private final OutputStreamWriter writer;
  private final DirectByteArrayOutputStream buffer;

  private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT =
      ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));
  private static final ThreadLocal<SimpleDateFormat> TIMESTAMP_FORMAT =
      ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));

  private static final class DirectByteArrayOutputStream extends ByteArrayOutputStream {
    DirectByteArrayOutputStream(int size) {
      super(size);
    }

    public byte[] getRawBuffer() {
      return this.buf;
    }
  }

  public CopyProtocolBatchWriter(CopyIn copyInOperation, String[] fieldNames) {
    this.copyInOperation = checkNotNull(copyInOperation, "copyInOperation must not be null");
    checkNotNull(fieldNames, "fieldNames must not be null");

    this.currentRow = new String[fieldNames.length];
    this.buffer = new DirectByteArrayOutputStream(4096);
    this.writer = new OutputStreamWriter(this.buffer, StandardCharsets.UTF_8);
  }

  @Override
  public void addBatch() throws Exception {
    writer.write(currentRow[0]);
    for (int i = 1; i < currentRow.length; i++) {
      writer.write(',');
      writer.write(currentRow[i]);
    }
    writer.write('\n');
  }

  @Override
  public int[] executeBatch() throws Exception {
    writer.flush();

    copyInOperation.writeToCopy(buffer.getRawBuffer(), 0, buffer.size());

    buffer.reset();
    return new int[0];
  }

  @Override
  public void close() throws Exception {
    try {
      copyInOperation.endCopy();
    } finally {
      if (copyInOperation.isActive()) {
        copyInOperation.cancelCopy();
      }
    }
  }

  @Override
  public void setNull(int fieldIndex, int sqlType) {
    currentRow[fieldIndex] = "";
  }

  @Override
  public void setBoolean(int fieldIndex, boolean x) {
    currentRow[fieldIndex] = String.valueOf(x);
  }

  @Override
  public void setByte(int fieldIndex, byte x) {
    currentRow[fieldIndex] = String.valueOf(x);
  }

  @Override
  public void setShort(int fieldIndex, short x) {
    currentRow[fieldIndex] = String.valueOf(x);
  }

  @Override
  public void setInt(int fieldIndex, int x) {
    currentRow[fieldIndex] = String.valueOf(x);
  }

  @Override
  public void setLong(int fieldIndex, long x) {
    currentRow[fieldIndex] = String.valueOf(x);
  }

  @Override
  public void setFloat(int fieldIndex, float x) {
    currentRow[fieldIndex] = String.valueOf(x);
  }

  @Override
  public void setDouble(int fieldIndex, double x) {
    currentRow[fieldIndex] = String.valueOf(x);
  }

  @Override
  public void setBigDecimal(int fieldIndex, BigDecimal x) {
    currentRow[fieldIndex] = x.toPlainString();
  }

  @Override
  public void setString(int fieldIndex, String x) {
    currentRow[fieldIndex] = "\"" + x.replace("\"", "\"\"") + "\"";
  }

  @Override
  public void setBytes(int fieldIndex, byte[] x) {
    currentRow[fieldIndex] = formatBytesToHex(x);
  }

  @Override
  public void setDate(int fieldIndex, Date x) {
    currentRow[fieldIndex] = DATE_FORMAT.get().format(x);
  }

  @Override
  public void setTime(int fieldIndex, Time x) {
    currentRow[fieldIndex] = x.toString();
  }

  @Override
  public void setTimestamp(int fieldIndex, Timestamp x) {
    currentRow[fieldIndex] = TIMESTAMP_FORMAT.get().format(x);
  }

  private String formatBytesToHex(byte[] bytes) {
    StringBuilder hex = new StringBuilder("\\\\x");
    for (byte b : bytes) {
      hex.append(String.format("%02x", b));
    }
    return hex.toString();
  }
}
