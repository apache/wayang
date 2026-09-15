/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.jdbc.server;

import org.apache.wayang.api.sql.context.SqlColumn;
import org.apache.wayang.api.sql.context.SqlQueryResult;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.jdbc.protocol.message.ColumnInfo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts SQL API results into JDBC protocol result structures.
 */
final class SqlQueryResultAdapter {

    private SqlQueryResultAdapter() {
    }

    static List<ColumnInfo> toColumnInfo(final SqlQueryResult result) {
        return result.getColumns().stream()
                .map(SqlQueryResultAdapter::toColumnInfo)
                .collect(Collectors.toList());
    }

    static List<List<Object>> toRows(final SqlQueryResult result) {
        final List<SqlColumn> columns = result.getColumns();
        final List<List<Object>> rows = new ArrayList<>();
        for (final Record record : result.getRows()) {
            final Object[] values = record.getValues();
            if (values.length != columns.size()) {
                throw new IllegalStateException(
                        "Query result row width does not match its column count."
                );
            }
            final List<Object> row = new ArrayList<>(values.length);
            for (int index = 0; index < values.length; index++) {
                row.add(normalizeWireValue(values[index], columns.get(index)));
            }
            rows.add(row);
        }
        return rows;
    }

    /**
     * Converts values that Jackson cannot represent portably without type
     * information into stable JDBC wire representations. Numeric values stay
     * numeric so that the driver can coerce them using {@link ColumnInfo};
     * temporal values use ISO text to avoid default-time-zone shifts.
     */
    private static Object normalizeWireValue(final Object value, final SqlColumn column) {
        if (value == null) {
            return value;
        }

        final Object normalizedValue;
        switch (column.getJdbcType()) {
            case Types.DATE:
                if (value instanceof java.sql.Date) {
                    normalizedValue = ((java.sql.Date) value).toLocalDate().toString();
                    break;
                }
                if (value instanceof LocalDate) {
                    normalizedValue = value.toString();
                    break;
                }
                normalizedValue = value;
                break;
            case Types.TIME:
                if (value instanceof Time) {
                    normalizedValue = ((Time) value).toLocalTime().toString();
                    break;
                }
                if (value instanceof LocalTime) {
                    normalizedValue = value.toString();
                    break;
                }
                normalizedValue = value;
                break;
            case Types.TIME_WITH_TIMEZONE:
                if (value instanceof OffsetTime) {
                    normalizedValue = value.toString();
                    break;
                }
                normalizedValue = value;
                break;
            case Types.TIMESTAMP:
                if (value instanceof Timestamp) {
                    normalizedValue = ((Timestamp) value).toLocalDateTime().toString();
                    break;
                }
                if (value instanceof LocalDateTime || value instanceof Instant) {
                    normalizedValue = value.toString();
                    break;
                }
                normalizedValue = value;
                break;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                if (value instanceof OffsetDateTime || value instanceof Instant) {
                    normalizedValue = value.toString();
                    break;
                }
                if (value instanceof ZonedDateTime) {
                    normalizedValue = ((ZonedDateTime) value).toOffsetDateTime().toString();
                    break;
                }
                normalizedValue = value;
                break;
            default:
                normalizedValue = value;
                break;
        }
        return requireWireScalar(normalizedValue, column);
    }

    private static Object requireWireScalar(final Object value, final SqlColumn column) {
        if (value == null || value instanceof String || value instanceof Boolean) {
            return value;
        }
        if (value instanceof Character) {
            return value.toString();
        }
        if (value instanceof byte[]) {
            return ((byte[]) value).clone();
        }
        if (value instanceof Double && !Double.isFinite((Double) value)) {
            throw unsupportedWireValue(value, column);
        }
        if (value instanceof Float && !Float.isFinite((Float) value)) {
            throw unsupportedWireValue(value, column);
        }
        if (value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Float
                || value instanceof Double
                || value instanceof BigInteger
                || value instanceof BigDecimal) {
            return value;
        }
        throw unsupportedWireValue(value, column);
    }

    private static IllegalStateException unsupportedWireValue(
            final Object value,
            final SqlColumn column
    ) {
        return new IllegalStateException(
                "Query result column '"
                        + column.getLabel()
                        + "' contains a value that cannot be represented by the JDBC protocol: "
                        + value.getClass().getName()
        );
    }

    private static ColumnInfo toColumnInfo(final SqlColumn column) {
        return new ColumnInfo(
                column.getName(),
                column.getLabel(),
                null,
                null,
                column.getTypeName(),
                column.getJdbcType(),
                column.isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls,
                column.getPrecision(),
                column.getScale()
        );
    }
}
