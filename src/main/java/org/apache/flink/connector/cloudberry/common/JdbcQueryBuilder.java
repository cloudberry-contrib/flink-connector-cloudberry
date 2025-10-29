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

import static java.lang.String.format;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.util.Preconditions;

public class JdbcQueryBuilder {
  // Define MAX/MIN precision of TIMESTAMP type according to PostgreSQL docs:
  // https://www.postgresql.org/docs/12/datatype-datetime.html
  private static final int MAX_TIMESTAMP_PRECISION = 6;
  private static final int MIN_TIMESTAMP_PRECISION = 0;

  // Define MAX/MIN precision of DECIMAL type according to PostgreSQL docs:
  // https://www.postgresql.org/docs/12/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
  private static final int MAX_DECIMAL_PRECISION = 1000;
  private static final int MIN_DECIMAL_PRECISION = 1;

  private static final JdbcQueryBuilder INSTANCE = new JdbcQueryBuilder();

  private JdbcQueryBuilder() {}

  public static JdbcQueryBuilder getInstance() {
    return INSTANCE;
  }

  public String getLimitClause(long limit) {
    return "LIMIT " + limit;
  }

  /** Postgres upsert query. It use ON CONFLICT ... DO UPDATE SET.. to replace into Postgres. */
  public String getUpsertStatement(
      String tableName, String[] fieldNames, String[] uniqueKeyFields) {
    String uniqueColumns =
        Arrays.stream(uniqueKeyFields).map(this::quoteIdentifier).collect(Collectors.joining(", "));
    final Set<String> uniqueKeyFieldsSet = new HashSet<>(Arrays.asList(uniqueKeyFields));
    String updateClause =
        Arrays.stream(fieldNames)
            .filter(f -> !uniqueKeyFieldsSet.contains(f))
            .map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f))
            .collect(Collectors.joining(", "));
    String conflictAction =
        updateClause.isEmpty() ? " DO NOTHING" : String.format(" DO UPDATE SET %s", updateClause);
    return getInsertIntoStatement(tableName, fieldNames)
        + " ON CONFLICT ("
        + uniqueColumns
        + ")"
        + conflictAction;
  }

  public String quoteIdentifier(String identifier) {
    return identifier;
  }

  public Optional<Range> decimalPrecisionRange() {
    return Optional.of(Range.of(MIN_DECIMAL_PRECISION, MAX_DECIMAL_PRECISION));
  }

  public Optional<Range> timestampPrecisionRange() {
    return Optional.of(Range.of(MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
  }

  public Set<LogicalTypeRoot> supportedTypes() {
    // The data types used in PostgreSQL are list at:
    // https://www.postgresql.org/docs/12/datatype.html

    // TODO: We can't convert BINARY data type to
    //  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in
    // LegacyTypeInfoDataTypeConverter.

    return EnumSet.of(
        LogicalTypeRoot.CHAR,
        LogicalTypeRoot.VARCHAR,
        LogicalTypeRoot.BOOLEAN,
        LogicalTypeRoot.VARBINARY,
        LogicalTypeRoot.DECIMAL,
        LogicalTypeRoot.TINYINT,
        LogicalTypeRoot.SMALLINT,
        LogicalTypeRoot.INTEGER,
        LogicalTypeRoot.BIGINT,
        LogicalTypeRoot.FLOAT,
        LogicalTypeRoot.DOUBLE,
        LogicalTypeRoot.DATE,
        LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
        LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
        LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        LogicalTypeRoot.ARRAY);
  }

  public void validate(RowType rowType) throws ValidationException {
    for (RowType.RowField field : rowType.getFields()) {
      // TODO: We can't convert VARBINARY(n) data type to
      //  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in
      //  LegacyTypeInfoDataTypeConverter when n is smaller
      //  than Integer.MAX_VALUE
      if (!supportedTypes().contains(field.getType().getTypeRoot())
          || (field.getType() instanceof VarBinaryType
              && Integer.MAX_VALUE != ((VarBinaryType) field.getType()).getLength())) {
        throw new ValidationException(
            format("Cloudberry doesn't support type: %s.", field.getType()));
      }

      if (field.getType() instanceof DecimalType) {
        Range range =
            decimalPrecisionRange()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            format(
                                "Cloudberry supports DECIMAL type but no precision range has been set. "
                                    + "Ensure AbstractDialect#decimalPrecisionRange() is overriden to return a valid Range")));
        int precision = ((DecimalType) field.getType()).getPrecision();
        if (precision > range.max || precision < range.min) {
          throw new ValidationException(
              format(
                  "The precision of field '%s' is out of the DECIMAL "
                      + "precision range [%d, %d] supported by %s Cloudberry.",
                  field.getName(), range.min, range.max));
        }
      }

      if (field.getType() instanceof TimestampType) {
        Range range =
            timestampPrecisionRange()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            format(
                                "Cloudberry supports TIMESTAMP type but no precision range has been set."
                                    + "Ensure AbstractDialect#timestampPrecisionRange() is overriden to return a valid Range")));
        int precision = ((TimestampType) field.getType()).getPrecision();
        if (precision > range.max || precision < range.min) {
          throw new ValidationException(
              format(
                  "The precision of field '%s' is out of the TIMESTAMP "
                      + "precision range [%d, %d] supported by Cloudberry.",
                  field.getName(), range.min, range.max));
        }
      }
    }
  }

  /**
   * A simple {@code INSERT INTO} statement.
   *
   * <pre>{@code
   * INSERT INTO table_name (column_name [, ...])
   * VALUES (?, ?, ...)
   * }</pre>
   */
  public String getInsertIntoStatement(String tableName, String[] fieldNames) {
    String columns =
        Arrays.stream(fieldNames).map(this::quoteIdentifier).collect(Collectors.joining(", "));
    String placeholders = Arrays.stream(fieldNames).map(f -> "?").collect(Collectors.joining(", "));
    return "INSERT INTO "
        + quoteIdentifier(tableName)
        + "("
        + columns
        + ")"
        + " VALUES ("
        + placeholders
        + ")";
  }

  /**
   * A simple single row {@code UPDATE} statement.
   *
   * <pre>{@code
   * UPDATE table_name
   * SET col = ? [, ...]
   * WHERE cond = ? [AND ...]
   * }</pre>
   */
  public String getUpdateStatement(
      String tableName, String[] fieldNames, String[] conditionFields) {
    String setClause =
        Arrays.stream(fieldNames)
            .map(f -> format("%s = ?", quoteIdentifier(f)))
            .collect(Collectors.joining(", "));
    String conditionClause =
        Arrays.stream(conditionFields)
            .map(f -> format("%s = ?", quoteIdentifier(f)))
            .collect(Collectors.joining(" AND "));
    return "UPDATE "
        + quoteIdentifier(tableName)
        + " SET "
        + setClause
        + " WHERE "
        + conditionClause;
  }

  /**
   * A simple single row {@code DELETE} statement.
   *
   * <pre>{@code
   * DELETE FROM table_name
   * WHERE cond = ? [AND ...]
   * }</pre>
   */
  public String getDeleteStatement(String tableName, String[] conditionFields) {
    String conditionClause =
        Arrays.stream(conditionFields)
            .map(f -> format("%s = ?", quoteIdentifier(f)))
            .collect(Collectors.joining(" AND "));
    return "DELETE FROM " + quoteIdentifier(tableName) + " WHERE " + conditionClause;
  }

  /**
   * A simple {@code SELECT} statement.
   *
   * <pre>{@code
   * SELECT expression [, ...]
   * FROM table_name
   * WHERE cond = ? [AND ...]
   * }</pre>
   */
  public String getSelectFromStatement(
      String tableName, String[] selectFields, String[] conditionFields) {
    String selectExpressions =
        Arrays.stream(selectFields).map(this::quoteIdentifier).collect(Collectors.joining(", "));
    String fieldExpressions =
        Arrays.stream(conditionFields)
            .map(f -> format("%s = ?", quoteIdentifier(f)))
            .collect(Collectors.joining(" AND "));
    return "SELECT "
        + selectExpressions
        + " FROM "
        + quoteIdentifier(tableName)
        + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
  }

  /**
   * A simple {@code SELECT} statement that checks for the existence of a single row.
   *
   * <pre>{@code
   * SELECT 1
   * FROM table_name
   * WHERE cond = ? [AND ...]
   * }</pre>
   */
  public String getRowExistsStatement(String tableName, String[] conditionFields) {
    String fieldExpressions =
        Arrays.stream(conditionFields)
            .map(f -> format("%s = ?", quoteIdentifier(f)))
            .collect(Collectors.joining(" AND "));
    return "SELECT 1 FROM " + quoteIdentifier(tableName) + " WHERE " + fieldExpressions;
  }

  /** A range from [min,max] where min <= max. */
  @PublicEvolving
  public static class Range {
    private final int min;

    private final int max;

    public static Range of(int min, int max) {
      Preconditions.checkArgument(
          min <= max,
          String.format("The range min value in range %d must be <= max value %d", min, max));
      return new Range(min, max);
    }

    private Range(int min, int max) {
      this.min = min;
      this.max = max;
    }
  }
}
