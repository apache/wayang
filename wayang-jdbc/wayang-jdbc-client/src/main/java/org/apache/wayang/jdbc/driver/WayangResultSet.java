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

package org.apache.wayang.jdbc.driver;

import org.apache.wayang.jdbc.protocol.message.ColumnInfo;
import org.apache.wayang.jdbc.protocol.message.FetchResponse;
import org.apache.wayang.jdbc.protocol.message.QueryResultResponse;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class WayangResultSet implements InvocationHandler {

    private final WayangStatement statement;

    private final WayangJdbcClient client;

    private final List<ColumnInfo> columns;

    private final ResultSetMetaData metaData;

    private final int maxRows;

    private List<List<Object>> rows;

    private String cursorId;

    private boolean hasMoreRows;

    private int fetchSize;

    private int rowIndex = -1;

    private int rowNumber;

    private CursorState cursorState;

    private volatile boolean closed;

    private Object lastValue;

    private boolean columnWasRead;

    private WayangResultSet(
            final WayangStatement statement,
            final WayangJdbcClient client,
            final QueryResultResponse response,
            final int fetchSize,
            final int maxRows
    ) throws SQLException {
        this.statement = statement;
        this.client = client;
        this.fetchSize = fetchSize;
        this.maxRows = maxRows;
        this.validateInitialResponse(response);
        this.columns = this.copyColumns(response.getColumns());
        this.rows = this.copyRows(response.getRows(), "query result");
        this.cursorId = this.normalizeCursorId(response.getCursorId());
        this.hasMoreRows = response.isHasMoreRows();
        this.cursorState = this.rows.isEmpty() ? CursorState.EMPTY : CursorState.BEFORE_FIRST;
        this.metaData = new WayangResultSetMetaData(this.columns);
    }

    static ResultSet create(
            final WayangStatement statement,
            final WayangJdbcClient client,
            final QueryResultResponse response,
            final int fetchSize,
            final int maxRows
    ) throws SQLException {
        return (ResultSet) Proxy.newProxyInstance(
                ResultSet.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                new WayangResultSet(statement, client, response, fetchSize, maxRows)
        );
    }

    @Override
    public Object invoke(
            final Object proxy,
            final Method method,
            final Object[] args
    ) throws Throwable {
        if (method.getDeclaringClass() == Object.class) {
            return this.invokeObjectMethod(proxy, method, args);
        }

        final String methodName = method.getName();
        if ("close".equals(methodName)) {
            this.close((ResultSet) proxy);
            return null;
        }
        if ("isClosed".equals(methodName)) {
            return this.isEffectivelyClosed();
        }

        this.ensureOpen();

        switch (methodName) {
            case "next":
                return this.next();
            case "wasNull":
                return this.columnWasRead && this.lastValue == null;
            case "getMetaData":
                return this.metaData;
            case "getStatement":
                return this.statement;
            case "findColumn":
                return this.findColumn((String) args[0]);
            case "getObject":
                return this.getObject(method, args);
            case "getString":
            case "getNString":
                return this.asString(this.value(args[0]));
            case "getBoolean":
                return this.asBoolean(this.value(args[0]));
            case "getByte":
                return this.asBoundedIntegral(
                        this.value(args[0]),
                        Byte.MIN_VALUE,
                        Byte.MAX_VALUE,
                        "byte"
                ).byteValue();
            case "getShort":
                return this.asBoundedIntegral(
                        this.value(args[0]),
                        Short.MIN_VALUE,
                        Short.MAX_VALUE,
                        "short"
                ).shortValue();
            case "getInt":
                return this.asBoundedIntegral(
                        this.value(args[0]),
                        Integer.MIN_VALUE,
                        Integer.MAX_VALUE,
                        "int"
                ).intValue();
            case "getLong":
                return this.asBoundedIntegral(
                        this.value(args[0]),
                        Long.MIN_VALUE,
                        Long.MAX_VALUE,
                        "long"
                ).longValue();
            case "getFloat":
                return this.asFloat(this.value(args[0]));
            case "getDouble":
                return this.asDouble(this.value(args[0]));
            case "getBigDecimal":
                return this.getBigDecimal(args);
            case "getBytes":
                return this.asBytes(this.value(args[0]));
            case "getAsciiStream":
                return this.asAsciiStream(this.value(args[0]));
            case "getBinaryStream":
                return this.asBinaryStream(this.value(args[0]));
            case "getUnicodeStream":
                return this.asUnicodeStream(this.value(args[0]));
            case "getCharacterStream":
            case "getNCharacterStream":
                return this.asCharacterStream(this.value(args[0]));
            case "getDate":
                return this.asDate(this.value(args[0]), this.calendarArg(args));
            case "getTime":
                return this.asTime(this.value(args[0]), this.calendarArg(args));
            case "getTimestamp":
                return this.asTimestamp(this.value(args[0]), this.calendarArg(args));
            case "getURL":
                return this.asUrl(this.value(args[0]));
            case "getArray":
                return this.unsupportedValue("SQL ARRAY values are not supported.");
            case "getBlob":
                return this.unsupportedValue("BLOB values are not supported.");
            case "getClob":
                return this.unsupportedValue("CLOB values are not supported.");
            case "getNClob":
                return this.unsupportedValue("NCLOB values are not supported.");
            case "getSQLXML":
                return this.unsupportedValue("SQLXML values are not supported.");
            case "getRef":
                return this.unsupportedValue("SQL REF values are not supported.");
            case "getRowId":
                return this.unsupportedValue("ROWID values are not supported.");
            case "getCursorName":
                return this.unsupportedValue("Named cursors are not supported.");
            case "getRow":
                return this.cursorState == CursorState.ON_ROW ? this.rowNumber : 0;
            case "isBeforeFirst":
                return this.cursorState == CursorState.BEFORE_FIRST;
            case "isAfterLast":
                return this.cursorState == CursorState.AFTER_LAST;
            case "isFirst":
                return this.cursorState == CursorState.ON_ROW && this.rowNumber == 1;
            case "isLast":
                return this.isOnLastRow();
            case "rowUpdated":
            case "rowInserted":
            case "rowDeleted":
                return false;
            case "beforeFirst":
            case "afterLast":
            case "first":
            case "last":
            case "absolute":
            case "relative":
            case "previous":
                throw this.unsupported("Wayang JDBC supports only forward-only result sets.");
            case "getType":
                return ResultSet.TYPE_FORWARD_ONLY;
            case "getConcurrency":
                return ResultSet.CONCUR_READ_ONLY;
            case "getHoldability":
                return ResultSet.CLOSE_CURSORS_AT_COMMIT;
            case "getFetchDirection":
                return ResultSet.FETCH_FORWARD;
            case "setFetchDirection":
                this.validateFetchDirection((Integer) args[0]);
                if ((Integer) args[0] != ResultSet.FETCH_FORWARD) {
                    throw this.unsupported("Wayang JDBC supports only forward fetch direction.");
                }
                return null;
            case "getFetchSize":
                return this.fetchSize;
            case "setFetchSize":
                this.setFetchSize((Integer) args[0]);
                return null;
            case "getWarnings":
                return null;
            case "clearWarnings":
                return null;
            case "unwrap":
                return this.unwrap(proxy, (Class<?>) args[0]);
            case "isWrapperFor":
                return this.isWrapperFor(proxy, (Class<?>) args[0]);
            default:
                if (this.isWriteMethod(methodName)) {
                    throw this.unsupported("Wayang JDBC result sets are read-only.");
                }
                throw this.unsupported("ResultSet method is not supported yet: " + methodName);
        }
    }

    private Object invokeObjectMethod(
            final Object proxy,
            final Method method,
            final Object[] args
    ) {
        switch (method.getName()) {
            case "toString":
                return "WayangResultSet";
            case "hashCode":
                return System.identityHashCode(proxy);
            case "equals":
                return proxy == args[0];
            default:
                throw new IllegalStateException("Unsupported Object method: " + method.getName());
        }
    }

    private boolean next() throws SQLException {
        this.clearLastValue();

        if (this.cursorState == CursorState.AFTER_LAST || this.cursorState == CursorState.EMPTY) {
            return false;
        }

        if (this.reachedMaxRows()) {
            this.finishResultSet();
            return false;
        }

        if (this.rowIndex + 1 < this.rows.size()) {
            this.rowIndex++;
            this.rowNumber++;
            this.cursorState = CursorState.ON_ROW;
            return true;
        }

        while (this.hasMoreRows && this.cursorId != null) {
            this.fetchNextBatch();

            if (this.reachedMaxRows()) {
                this.finishResultSet();
                return false;
            }

            if (this.rowIndex + 1 < this.rows.size()) {
                this.rowIndex++;
                this.rowNumber++;
                this.cursorState = CursorState.ON_ROW;
                return true;
            }
        }

        this.finishResultSet();
        return false;
    }

    private void fetchNextBatch() throws SQLException {
        final String requestedCursorId = this.cursorId;
        final FetchResponse response = this.client.fetch(requestedCursorId, this.fetchSize);
        try {
            if (response == null) {
                throw this.protocolError("Wayang JDBC server returned a null fetch response.");
            }
            if (!this.client.getConnectionId().equals(response.getConnectionId())) {
                throw this.protocolError("Fetch response connection id does not match the open connection.");
            }
            if (!requestedCursorId.equals(response.getCursorId())) {
                throw this.protocolError("Fetch response cursor id does not match the requested cursor.");
            }

            final List<List<Object>> fetchedRows = this.copyRows(response.getRows(), "fetch response");
            if (response.isHasMoreRows() && fetchedRows.isEmpty()) {
                throw this.protocolError("Fetch response reported more rows but returned an empty batch.");
            }

            this.rows = fetchedRows;
            this.hasMoreRows = response.isHasMoreRows();
            this.rowIndex = -1;
            if (!this.hasMoreRows) {
                this.cursorId = null;
            }
        } catch (SQLException e) {
            this.closeCursorAfterProtocolFailure(e);
            throw e;
        }
    }

    private void finishResultSet() throws SQLException {
        this.closeCursorIfOpen();
        this.rowIndex = -1;
        this.clearLastValue();
        this.cursorState = this.rowNumber == 0 ? CursorState.EMPTY : CursorState.AFTER_LAST;
    }

    private boolean isOnLastRow() {
        if (this.cursorState != CursorState.ON_ROW) {
            return false;
        }
        if (this.reachedMaxRows()) {
            return true;
        }
        return !this.hasMoreRows && this.rowIndex == this.rows.size() - 1;
    }

    private void clearLastValue() {
        this.lastValue = null;
        this.columnWasRead = false;
    }

    private Object getObject(final Method method, final Object[] args) throws SQLException {
        final int columnIndex = this.columnIndex(args[0]);
        final Object value = this.value(columnIndex);
        if (args.length == 1) {
            return this.defaultJdbcObject(value, this.columns.get(columnIndex - 1));
        }

        if (Map.class.isAssignableFrom(method.getParameterTypes()[1])) {
            throw this.unsupported("Custom SQL type maps are not supported.");
        }

        if (!(args[1] instanceof Class<?>)) {
            throw new SQLException("Target type must not be null.");
        }
        final Class<?> targetType = (Class<?>) args[1];
        if (value == null || targetType.isInstance(value)) {
            return value;
        }
        if (targetType == String.class) {
            return this.asString(value);
        }
        if (targetType == Boolean.class || targetType == boolean.class) {
            return this.asBoolean(value);
        }
        if (targetType == Byte.class || targetType == byte.class) {
            return this.asBoundedIntegral(
                    value,
                    Byte.MIN_VALUE,
                    Byte.MAX_VALUE,
                    "byte"
            ).byteValue();
        }
        if (targetType == Short.class || targetType == short.class) {
            return this.asBoundedIntegral(
                    value,
                    Short.MIN_VALUE,
                    Short.MAX_VALUE,
                    "short"
            ).shortValue();
        }
        if (targetType == Integer.class || targetType == int.class) {
            return this.asBoundedIntegral(
                    value,
                    Integer.MIN_VALUE,
                    Integer.MAX_VALUE,
                    "int"
            ).intValue();
        }
        if (targetType == Long.class || targetType == long.class) {
            return this.asBoundedIntegral(
                    value,
                    Long.MIN_VALUE,
                    Long.MAX_VALUE,
                    "long"
            ).longValue();
        }
        if (targetType == Float.class || targetType == float.class) {
            return this.asFloat(value);
        }
        if (targetType == Double.class || targetType == double.class) {
            return this.asDouble(value);
        }
        if (targetType == BigDecimal.class) {
            return this.asBigDecimal(value);
        }
        if (targetType == byte[].class) {
            return this.asBytes(value);
        }
        if (targetType == Date.class) {
            return this.asDate(value, null);
        }
        if (targetType == Time.class) {
            return this.asTime(value, null);
        }
        if (targetType == Timestamp.class) {
            return this.asTimestamp(value, null);
        }
        if (targetType == LocalDate.class) {
            final Date date = this.asDate(value, null);
            return date == null ? null : date.toLocalDate();
        }
        if (targetType == LocalTime.class) {
            final Time time = this.asTime(value, null);
            return time == null ? null : time.toLocalTime();
        }
        if (targetType == LocalDateTime.class) {
            final Timestamp timestamp = this.asTimestamp(value, null);
            return timestamp == null ? null : timestamp.toLocalDateTime();
        }
        if (targetType == Instant.class) {
            final Timestamp timestamp = this.asTimestamp(value, null);
            return timestamp == null ? null : timestamp.toInstant();
        }
        if (targetType == URL.class) {
            return this.asUrl(value);
        }
        throw new SQLException("Cannot convert value to " + targetType.getName());
    }

    private Object defaultJdbcObject(
            final Object value,
            final ColumnInfo column
    ) throws SQLException {
        if (value == null) {
            return null;
        }
        switch (column.getJdbcType()) {
            case Types.DATE:
                return this.asDate(value, null);
            case Types.TIME:
                return this.asTime(value, null);
            case Types.TIMESTAMP:
                return this.asTimestamp(value, null);
            default:
                return value;
        }
    }

    private BigDecimal getBigDecimal(final Object[] args) throws SQLException {
        final BigDecimal value = this.asBigDecimal(this.value(args[0]));
        if (value == null || args.length == 1) {
            return value;
        }
        final int scale = (Integer) args[1];
        if (scale < 0) {
            throw new SQLException("Scale must not be negative.");
        }
        return value.setScale(scale, RoundingMode.HALF_UP);
    }

    private Object value(final Object column) throws SQLException {
        this.ensureOpen();
        final int columnIndex = this.columnIndex(column);
        final List<Object> row = this.currentRow();
        this.lastValue = row.get(columnIndex - 1);
        this.columnWasRead = true;
        return this.lastValue;
    }

    private int columnIndex(final Object column) throws SQLException {
        final int columnIndex;
        if (column instanceof Integer) {
            columnIndex = (Integer) column;
        } else if (column instanceof String) {
            columnIndex = this.findColumn((String) column);
        } else {
            throw new SQLException("Unsupported column reference: " + column);
        }

        if (columnIndex < 1 || columnIndex > this.columns.size()) {
            throw new SQLException("Column index out of bounds: " + columnIndex);
        }
        return columnIndex;
    }

    private List<Object> currentRow() throws SQLException {
        if (this.cursorState != CursorState.ON_ROW
                || this.rowIndex < 0
                || this.rowIndex >= this.rows.size()) {
            throw new SQLException("ResultSet cursor is not positioned on a row.");
        }
        return this.rows.get(this.rowIndex);
    }

    private int findColumn(final String columnLabel) throws SQLException {
        if (columnLabel == null || columnLabel.isBlank()) {
            throw new SQLException("Column label must not be blank.");
        }
        for (int i = 0; i < this.columns.size(); i++) {
            final ColumnInfo column = this.columns.get(i);
            if (columnLabel.equalsIgnoreCase(column.getColumnLabel())
                    || columnLabel.equalsIgnoreCase(column.getColumnName())) {
                return i + 1;
            }
        }
        throw new SQLException("Unknown column label: " + columnLabel);
    }

    private void close(final ResultSet proxy) throws SQLException {
        if (this.closed) {
            return;
        }

        SQLException failure = null;
        try {
            this.closeCursorIfOpen();
        } catch (SQLException e) {
            failure = e;
        } finally {
            this.closed = true;
            try {
                this.statement.resultSetClosed(proxy);
            } catch (SQLException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    private List<ColumnInfo> copyColumns(final List<ColumnInfo> sourceColumns) throws SQLException {
        if (sourceColumns == null || sourceColumns.isEmpty()) {
            throw this.protocolError("Query result did not declare any columns.");
        }
        final List<ColumnInfo> copy = new ArrayList<>(sourceColumns.size());
        for (int index = 0; index < sourceColumns.size(); index++) {
            final ColumnInfo column = sourceColumns.get(index);
            if (column == null) {
                throw this.protocolError("Query result column " + (index + 1) + " is null.");
            }
            copy.add(column);
        }
        return Collections.unmodifiableList(copy);
    }

    private List<List<Object>> copyRows(
            final List<List<Object>> sourceRows,
            final String responseName
    ) throws SQLException {
        if (sourceRows == null) {
            throw this.protocolError("Wayang JDBC server returned null rows in " + responseName + ".");
        }
        final List<List<Object>> copy = new ArrayList<>();
        for (int index = 0; index < sourceRows.size(); index++) {
            final List<Object> row = sourceRows.get(index);
            if (row == null) {
                throw this.protocolError(responseName + " row " + (index + 1) + " is null.");
            }
            if (row.size() != this.columns.size()) {
                throw this.protocolError(responseName + " row " + (index + 1)
                        + " has " + row.size() + " values but "
                        + this.columns.size() + " columns were declared.");
            }
            final List<Object> convertedRow = new ArrayList<>(row.size());
            for (int columnIndex = 0; columnIndex < row.size(); columnIndex++) {
                convertedRow.add(this.coerceJdbcValue(
                        row.get(columnIndex),
                        this.columns.get(columnIndex)
                ));
            }
            copy.add(convertedRow);
        }
        return copy;
    }

    private Object coerceJdbcValue(
            final Object value,
            final ColumnInfo column
    ) throws SQLException {
        if (value == null) {
            return null;
        }

        try {
            switch (column.getJdbcType()) {
                case Types.BIT:
                case Types.BOOLEAN:
                    return this.asBoolean(value);
                case Types.TINYINT:
                    return this.asBigDecimal(value).byteValueExact();
                case Types.SMALLINT:
                    return this.asBigDecimal(value).shortValueExact();
                case Types.INTEGER:
                    return this.asBigDecimal(value).intValueExact();
                case Types.BIGINT:
                    return this.asBigDecimal(value).longValueExact();
                case Types.REAL:
                    return this.asNumber(value).floatValue();
                case Types.FLOAT:
                case Types.DOUBLE:
                    return this.asNumber(value).doubleValue();
                case Types.NUMERIC:
                case Types.DECIMAL:
                    return this.asBigDecimal(value);
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    return this.asString(value);
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    return this.decodeBinaryValue(value);
                case Types.DATE:
                    return this.asLocalDateValue(value);
                case Types.TIME:
                    return this.asLocalTimeValue(value);
                case Types.TIMESTAMP:
                    return this.asLocalDateTimeValue(value);
                case Types.TIME_WITH_TIMEZONE:
                    return this.asOffsetTime(value);
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    return this.asOffsetDateTime(value);
                default:
                    return value;
            }
        } catch (ArithmeticException e) {
            throw this.protocolError(
                    "Value for column '" + column.getColumnLabel()
                            + "' is outside the declared JDBC type range."
            );
        }
    }

    private LocalDate asLocalDateValue(final Object value) throws SQLException {
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        }
        if (value instanceof Date) {
            return ((Date) value).toLocalDate();
        }
        if (value instanceof Number) {
            try {
                return LocalDate.ofEpochDay(((Number) value).longValue());
            } catch (RuntimeException e) {
                throw new SQLException("Cannot convert value to LocalDate: " + value, e);
            }
        }
        try {
            final String text = String.valueOf(value).trim();
            final int timeSeparator = text.indexOf(' ');
            final int isoSeparator = text.indexOf('T');
            final int separator = timeSeparator >= 0 ? timeSeparator : isoSeparator;
            return LocalDate.parse(separator >= 0 ? text.substring(0, separator) : text);
        } catch (RuntimeException e) {
            throw new SQLException("Cannot convert value to LocalDate: " + value, e);
        }
    }

    private LocalTime asLocalTimeValue(final Object value) throws SQLException {
        if (value instanceof LocalTime) {
            return (LocalTime) value;
        }
        if (value instanceof Time) {
            return ((Time) value).toLocalTime();
        }
        if (value instanceof Number) {
            final long millis = ((Number) value).longValue();
            if (millis < 0L || millis >= 86_400_000L) {
                throw new SQLException("TIME value is outside one day: " + value);
            }
            return LocalTime.ofNanoOfDay(millis * 1_000_000L);
        }
        try {
            String text = String.valueOf(value).trim();
            final int dateSeparator = text.indexOf('T');
            if (dateSeparator >= 0) {
                text = text.substring(dateSeparator + 1);
            } else if (text.indexOf(' ') >= 0) {
                text = text.substring(text.indexOf(' ') + 1);
            }
            return LocalTime.parse(text);
        } catch (RuntimeException e) {
            throw new SQLException("Cannot convert value to LocalTime: " + value, e);
        }
    }

    private LocalDateTime asLocalDateTimeValue(final Object value) throws SQLException {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof Timestamp) {
            return ((Timestamp) value).toLocalDateTime();
        }
        if (value instanceof Number) {
            return LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(((Number) value).longValue()),
                    ZoneOffset.UTC
            );
        }
        try {
            final String text = String.valueOf(value).trim();
            try {
                return LocalDateTime.parse(text.replace(' ', 'T'));
            } catch (RuntimeException ignored) {
                return LocalDateTime.ofInstant(Instant.parse(text), ZoneOffset.UTC);
            }
        } catch (RuntimeException e) {
            throw new SQLException("Cannot convert value to LocalDateTime: " + value, e);
        }
    }

    private byte[] decodeBinaryValue(final Object value) throws SQLException {
        if (value instanceof byte[]) {
            return ((byte[]) value).clone();
        }
        if (value instanceof String) {
            try {
                return Base64.getDecoder().decode((String) value);
            } catch (IllegalArgumentException e) {
                throw this.protocolError("Binary result value is not valid Base64 text.");
            }
        }
        throw this.protocolError("Binary result value has an unsupported wire representation.");
    }

    private OffsetTime asOffsetTime(final Object value) throws SQLException {
        if (value instanceof OffsetTime) {
            return (OffsetTime) value;
        }
        try {
            return OffsetTime.parse(String.valueOf(value).trim());
        } catch (RuntimeException e) {
            throw new SQLException("Cannot convert value to OffsetTime: " + value, e);
        }
    }

    private OffsetDateTime asOffsetDateTime(final Object value) throws SQLException {
        if (value instanceof OffsetDateTime) {
            return (OffsetDateTime) value;
        }
        if (value instanceof Instant) {
            return ((Instant) value).atOffset(java.time.ZoneOffset.UTC);
        }
        try {
            final String text = String.valueOf(value).trim();
            try {
                return OffsetDateTime.parse(text);
            } catch (RuntimeException ignored) {
                return Instant.parse(text).atOffset(java.time.ZoneOffset.UTC);
            }
        } catch (RuntimeException e) {
            throw new SQLException("Cannot convert value to OffsetDateTime: " + value, e);
        }
    }

    private boolean reachedMaxRows() {
        return this.maxRows > 0 && this.rowNumber >= this.maxRows;
    }

    private void closeCursorIfOpen() throws SQLException {
        if (this.cursorId != null) {
            if (!this.client.isClosed()) {
                this.client.closeCursor(this.statement.getStatementId(), this.cursorId);
            }
            this.cursorId = null;
            this.hasMoreRows = false;
        }
    }

    private String asString(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return new String((byte[]) value, StandardCharsets.UTF_8);
        }
        return String.valueOf(value);
    }

    private Boolean asBoolean(final Object value) throws SQLException {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0D;
        }
        final String text = String.valueOf(value).trim().toLowerCase(Locale.ROOT);
        if ("true".equals(text) || "1".equals(text)) {
            return true;
        }
        if ("false".equals(text) || "0".equals(text)) {
            return false;
        }
        try {
            return new BigDecimal(text).compareTo(BigDecimal.ZERO) != 0;
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert value to boolean: " + value, e);
        }
    }

    private Number asNumber(final Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return (Number) value;
        }
        if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        }
        try {
            return new BigDecimal(String.valueOf(value));
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert value to number: " + value, e);
        }
    }

    private BigDecimal asBigDecimal(final Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number || value instanceof Boolean || value instanceof String) {
            return new BigDecimal(String.valueOf(this.asNumber(value)));
        }
        throw new SQLException("Cannot convert value to BigDecimal: " + value);
    }

    private BigDecimal asBoundedIntegral(
            final Object value,
            final long minimum,
            final long maximum,
            final String targetName
    ) throws SQLException {
        final BigDecimal decimal = this.asBigDecimal(value);
        if (decimal == null) {
            return BigDecimal.ZERO;
        }
        if (decimal.compareTo(BigDecimal.valueOf(minimum)) < 0
                || decimal.compareTo(BigDecimal.valueOf(maximum)) > 0) {
            throw new SQLException("Numeric value is outside the " + targetName + " range: " + value);
        }
        return decimal;
    }

    private float asFloat(final Object value) throws SQLException {
        if (value == null) {
            return 0F;
        }
        final float converted = this.asNumber(value).floatValue();
        if (!Float.isFinite(converted)) {
            throw new SQLException("Numeric value is outside the float range: " + value);
        }
        return converted;
    }

    private double asDouble(final Object value) throws SQLException {
        if (value == null) {
            return 0D;
        }
        final double converted = this.asNumber(value).doubleValue();
        if (!Double.isFinite(converted)) {
            throw new SQLException("Numeric value is outside the double range: " + value);
        }
        return converted;
    }

    private byte[] asBytes(final Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return ((byte[]) value).clone();
        }
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    private java.io.InputStream asAsciiStream(final Object value) {
        final String text = this.asString(value);
        return text == null
                ? null
                : new java.io.ByteArrayInputStream(text.getBytes(StandardCharsets.US_ASCII));
    }

    private java.io.InputStream asBinaryStream(final Object value) throws SQLException {
        final byte[] bytes = this.asBytes(value);
        return bytes == null ? null : new java.io.ByteArrayInputStream(bytes);
    }

    private java.io.InputStream asUnicodeStream(final Object value) {
        final String text = this.asString(value);
        return text == null
                ? null
                : new java.io.ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    }

    private java.io.Reader asCharacterStream(final Object value) {
        final String text = this.asString(value);
        return text == null ? null : new java.io.StringReader(text);
    }

    private Date asDate(final Object value, final Calendar calendar) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return new Date(((Date) value).getTime());
        }
        if (value instanceof LocalDate) {
            return this.dateFromLocalDate((LocalDate) value, calendar);
        }
        if (value instanceof LocalDateTime) {
            return this.dateFromLocalDate(((LocalDateTime) value).toLocalDate(), calendar);
        }
        if (value instanceof java.util.Date) {
            return new Date(((java.util.Date) value).getTime());
        }
        if (value instanceof Number) {
            final long numericValue = ((Number) value).longValue();
            if (Math.abs(numericValue) < 10_000_000L) {
                return this.dateFromLocalDate(LocalDate.ofEpochDay(numericValue), calendar);
            }
            return new Date(numericValue);
        }
        try {
            final String text = String.valueOf(value).trim();
            final int timeSeparator = text.indexOf(' ');
            final int isoSeparator = text.indexOf('T');
            final int separator = timeSeparator >= 0 ? timeSeparator : isoSeparator;
            return this.dateFromLocalDate(
                    LocalDate.parse(separator >= 0 ? text.substring(0, separator) : text),
                    calendar
            );
        } catch (RuntimeException e) {
            throw new SQLException("Cannot convert value to Date: " + value, e);
        }
    }

    private Time asTime(final Object value, final Calendar calendar) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Time) {
            return new Time(((Time) value).getTime());
        }
        if (value instanceof LocalTime) {
            return this.timeFromLocalTime((LocalTime) value, calendar);
        }
        if (value instanceof OffsetTime) {
            return this.timeFromLocalTime(((OffsetTime) value).toLocalTime(), calendar);
        }
        if (value instanceof LocalDateTime) {
            return this.timeFromLocalTime(((LocalDateTime) value).toLocalTime(), calendar);
        }
        if (value instanceof java.util.Date) {
            return new Time(((java.util.Date) value).getTime());
        }
        if (value instanceof Number) {
            return new Time(((Number) value).longValue());
        }
        try {
            String text = String.valueOf(value).trim();
            final int dateSeparator = text.indexOf('T');
            if (dateSeparator >= 0) {
                text = text.substring(dateSeparator + 1);
            } else if (text.indexOf(' ') >= 0) {
                text = text.substring(text.indexOf(' ') + 1);
            }
            return this.timeFromLocalTime(LocalTime.parse(text), calendar);
        } catch (RuntimeException e) {
            throw new SQLException("Cannot convert value to Time: " + value, e);
        }
    }

    private Timestamp asTimestamp(final Object value, final Calendar calendar) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp) {
            final Timestamp timestamp = new Timestamp(((Timestamp) value).getTime());
            timestamp.setNanos(((Timestamp) value).getNanos());
            return timestamp;
        }
        if (value instanceof LocalDateTime) {
            return this.timestampFromLocalDateTime((LocalDateTime) value, calendar);
        }
        if (value instanceof LocalDate) {
            return this.timestampFromLocalDateTime(
                    ((LocalDate) value).atStartOfDay(),
                    calendar
            );
        }
        if (value instanceof OffsetDateTime) {
            return Timestamp.from(((OffsetDateTime) value).toInstant());
        }
        if (value instanceof Instant) {
            return Timestamp.from((Instant) value);
        }
        if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        }
        if (value instanceof Number) {
            return new Timestamp(((Number) value).longValue());
        }
        try {
            final String text = String.valueOf(value).trim();
            try {
                return Timestamp.from(Instant.parse(text));
            } catch (RuntimeException ignored) {
                return this.timestampFromLocalDateTime(
                        LocalDateTime.parse(text.replace(' ', 'T')),
                        calendar
                );
            }
        } catch (RuntimeException e) {
            throw new SQLException("Cannot convert value to Timestamp: " + value, e);
        }
    }

    private Date dateFromLocalDate(final LocalDate value, final Calendar calendar) {
        if (calendar == null) {
            return Date.valueOf(value);
        }
        final Calendar copy = (Calendar) calendar.clone();
        copy.clear();
        copy.set(value.getYear(), value.getMonthValue() - 1, value.getDayOfMonth());
        return new Date(copy.getTimeInMillis());
    }

    private Time timeFromLocalTime(final LocalTime value, final Calendar calendar) {
        if (calendar == null) {
            return Time.valueOf(value);
        }
        final Calendar copy = (Calendar) calendar.clone();
        copy.clear();
        copy.set(1970, Calendar.JANUARY, 1, value.getHour(), value.getMinute(), value.getSecond());
        copy.set(Calendar.MILLISECOND, value.getNano() / 1_000_000);
        return new Time(copy.getTimeInMillis());
    }

    private Timestamp timestampFromLocalDateTime(
            final LocalDateTime value,
            final Calendar calendar
    ) {
        if (calendar == null) {
            return Timestamp.valueOf(value);
        }
        final Calendar copy = (Calendar) calendar.clone();
        copy.clear();
        copy.set(
                value.getYear(),
                value.getMonthValue() - 1,
                value.getDayOfMonth(),
                value.getHour(),
                value.getMinute(),
                value.getSecond()
        );
        final Timestamp timestamp = new Timestamp(copy.getTimeInMillis());
        timestamp.setNanos(value.getNano());
        return timestamp;
    }

    private URL asUrl(final Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof URL) {
            return (URL) value;
        }
        try {
            return new URL(String.valueOf(value));
        } catch (MalformedURLException e) {
            throw new SQLException("Cannot convert value to URL: " + value, e);
        }
    }

    private Object unsupportedValue(final String message) throws SQLFeatureNotSupportedException {
        throw this.unsupported(message);
    }

    private Object unwrap(final Object proxy, final Class<?> iface) throws SQLException {
        if (iface == null) {
            throw new SQLException("Wrapper interface must not be null.");
        }
        if (iface.isInstance(proxy)) {
            return iface.cast(proxy);
        }
        throw new SQLException("ResultSet does not wrap " + iface.getName());
    }

    private boolean isWrapperFor(final Object proxy, final Class<?> iface) throws SQLException {
        if (iface == null) {
            throw new SQLException("Wrapper interface must not be null.");
        }
        return iface.isInstance(proxy);
    }

    private void ensureOpen() throws SQLException {
        if (this.isEffectivelyClosed()) {
            throw new SQLException("Wayang JDBC result set is closed.", "24000");
        }
    }

    private boolean isEffectivelyClosed() {
        return this.closed || this.statement.isClosed();
    }

    private Calendar calendarArg(final Object[] args) throws SQLException {
        if (args.length == 1) {
            return null;
        }
        if (!(args[1] instanceof Calendar)) {
            throw new SQLException("Calendar must not be null.");
        }
        return (Calendar) args[1];
    }

    private void setFetchSize(final int fetchSize) throws SQLException {
        if (fetchSize < 0) {
            throw new SQLException("Fetch size must not be negative.");
        }
        if (this.maxRows > 0 && fetchSize > this.maxRows) {
            throw new SQLException("Fetch size must not exceed the maximum row limit.");
        }
        this.fetchSize = fetchSize;
    }

    private void validateFetchDirection(final int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD
                && direction != ResultSet.FETCH_REVERSE
                && direction != ResultSet.FETCH_UNKNOWN) {
            throw new SQLException("Invalid fetch direction: " + direction);
        }
    }

    private void validateInitialResponse(final QueryResultResponse response) throws SQLException {
        if (response == null) {
            throw this.protocolError("Wayang JDBC server returned a null query response.");
        }
        if (!this.client.getConnectionId().equals(response.getConnectionId())) {
            throw this.protocolError("Query response connection id does not match the open connection.");
        }
        if (!this.statement.getStatementId().equals(response.getStatementId())) {
            throw this.protocolError("Query response statement id does not match the executing statement.");
        }
        final String responseCursorId = this.normalizeCursorId(response.getCursorId());
        if (response.isHasMoreRows() && responseCursorId == null) {
            throw this.protocolError("Query response reported more rows without a cursor id.");
        }
        if (!response.isHasMoreRows() && responseCursorId != null) {
            throw this.protocolError("Query response returned a cursor id without more rows.");
        }
        if (response.isHasMoreRows()
                && response.getRows() != null
                && response.getRows().isEmpty()) {
            throw this.protocolError("Query response reported more rows but returned an empty first batch.");
        }
        if (this.fetchSize < 0 || this.maxRows < 0) {
            throw new SQLException("Fetch size and maximum rows must not be negative.");
        }
    }

    private String normalizeCursorId(final String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private void closeCursorAfterProtocolFailure(final SQLException failure) {
        try {
            this.closeCursorIfOpen();
        } catch (SQLException closeException) {
            failure.addSuppressed(closeException);
        }
    }

    private boolean isWriteMethod(final String methodName) {
        return methodName.startsWith("update")
                || "insertRow".equals(methodName)
                || "deleteRow".equals(methodName)
                || "refreshRow".equals(methodName)
                || "cancelRowUpdates".equals(methodName)
                || "moveToInsertRow".equals(methodName)
                || "moveToCurrentRow".equals(methodName);
    }

    private SQLException protocolError(final String message) {
        return new SQLException(message, "08S01");
    }

    private SQLFeatureNotSupportedException unsupported(final String message) {
        return new SQLFeatureNotSupportedException(message, "0A000");
    }

    private enum CursorState {
        BEFORE_FIRST,
        ON_ROW,
        AFTER_LAST,
        EMPTY
    }
}
