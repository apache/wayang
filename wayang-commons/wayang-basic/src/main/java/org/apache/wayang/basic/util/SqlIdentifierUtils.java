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

package org.apache.wayang.basic.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * Utility for validating, escaping, and quoting SQL table and column identifiers.
 * Prevents SQL injection vulnerabilities (CWE-89) in DDL and DML operations.
 */
public final class SqlIdentifierUtils {

    /**
     * Pattern matching a single SQL identifier segment: must start with a letter or underscore,
     * followed by letters, digits, underscores, or dollar signs.
     */
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_$]*$");

    /**
     * Pattern matching valid table names: one or more identifier segments separated by dots
     * (e.g., "table", "schema.table", or "catalog.schema.table").
     */
    private static final Pattern TABLE_NAME_PATTERN =
            Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_$]*(?:\\.[a-zA-Z_][a-zA-Z0-9_$]*)*$");

    private SqlIdentifierUtils() {
        // Utility class
    }

    /**
     * Validates a single SQL identifier (e.g., column name or schema segment).
     *
     * @param identifier the identifier to validate
     * @throws IllegalArgumentException if the identifier is null, empty, or contains invalid characters
     */
    public static void validateIdentifier(String identifier) {
        if (identifier == null || identifier.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL identifier must not be null or blank.");
        }
        if (!IDENTIFIER_PATTERN.matcher(identifier).matches()) {
            throw new IllegalArgumentException(String.format(
                    "Invalid SQL identifier: '%s'. Identifiers must start with a letter or underscore "
                            + "and contain only alphanumeric characters, underscores, or dollar signs.",
                    identifier));
        }
    }

    /**
     * Validates a SQL table name, which may be single-part or multipart (e.g., schema.table).
     *
     * @param tableName the table name to validate
     * @throws IllegalArgumentException if the table name is null, empty, or contains invalid characters
     */
    public static void validateTableName(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL table name must not be null or blank.");
        }
        if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
            throw new IllegalArgumentException(String.format(
                    "Invalid SQL table name: '%s'. Table names must be valid identifiers, optionally "
                            + "separated by dots (e.g., 'table', 'schema.table', 'catalog.schema.table').",
                    tableName));
        }
    }

    /**
     * Validates an array of column names.
     *
     * @param columnNames array of column names to validate, may be null or empty
     * @throws IllegalArgumentException if any column name is invalid
     */
    public static void validateColumnNames(String[] columnNames) {
        if (columnNames == null) {
            return;
        }
        for (String columnName : columnNames) {
            validateIdentifier(columnName);
        }
    }

    /**
     * Validates, escapes, and quotes a table or column identifier using the database connection's
     * metadata. If the connection is null or fails to provide metadata, standard ANSI double-quotes
     * are used.
     *
     * <p>For multipart identifiers (e.g., {@code catalog.schema.table}), each segment is quoted
     * individually (e.g., {@code "catalog"."schema"."table"}).</p>
     *
     * @param identifier the table or column identifier to escape and quote
     * @param connection active JDBC connection, may be null
     * @return the escaped and quoted identifier
     */
    public static String escapeAndQuoteIdentifier(String identifier, Connection connection) {
        validateTableName(identifier);

        String quote = "\"";
        String closingQuote = "\"";

        if (connection != null) {
            try {
                DatabaseMetaData meta = connection.getMetaData();
                if (meta != null) {
                    String metaQuote = meta.getIdentifierQuoteString();
                    if (metaQuote != null && !metaQuote.trim().isEmpty()) {
                        quote = metaQuote.trim();
                        closingQuote = quote.equals("[") ? "]" : quote;
                    }
                }
            } catch (SQLException ignored) {
                // Fall back to default ANSI quoting
            }
        }

        return escapeAndQuoteIdentifier(identifier, quote, closingQuote, false, false);
    }

    /**
     * Validates, escapes, and quotes an identifier for a specific {@link DatabaseProduct}.
     *
     * @param identifier the identifier to escape and quote
     * @param product    the database product
     * @return the escaped and quoted identifier
     */
    public static String escapeAndQuoteIdentifier(String identifier, DatabaseProduct product) {
        validateTableName(identifier);

        String quote = (product == DatabaseProduct.MYSQL) ? "`"
                : (product == DatabaseProduct.MSSQL) ? "[" : "\"";
        String closingQuote = (product == DatabaseProduct.MSSQL) ? "]" : quote;
        return escapeAndQuoteIdentifier(identifier, quote, closingQuote, false, false);
    }

    /**
     * Validates, escapes, and quotes an identifier using the specified opening and closing quote strings.
     *
     * @param identifier   the identifier to escape and quote
     * @param quote        the opening quote string
     * @param closingQuote the closing quote string
     * @return the escaped and quoted identifier
     */
    public static String escapeAndQuoteIdentifier(String identifier, String quote, String closingQuote) {
        validateTableName(identifier);
        return escapeAndQuoteIdentifier(identifier, quote, closingQuote, false, false);
    }

    /**
     * Validates, escapes, and quotes an identifier with optional case folding.
     *
     * @param identifier   the identifier to escape and quote
     * @param quote        the opening quote string
     * @param closingQuote the closing quote string
     * @param toUpper      whether to fold unquoted identifiers to uppercase
     * @param toLower      whether to fold unquoted identifiers to lowercase
     * @return the escaped and quoted identifier
     */
    public static String escapeAndQuoteIdentifier(
            String identifier, String quote, String closingQuote, boolean toUpper, boolean toLower) {
        validateTableName(identifier);

        String q = (quote == null || quote.trim().isEmpty()) ? "\"" : quote;
        String cq = (closingQuote == null || closingQuote.trim().isEmpty()) ? q : closingQuote;

        String[] parts = identifier.split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                sb.append(".");
            }
            String part = parts[i];
            if (toUpper) {
                part = part.toUpperCase();
            } else if (toLower) {
                part = part.toLowerCase();
            }

            // Escape any occurrence of closingQuote within the identifier part by doubling it
            String escapedPart;
            if ("]".equals(cq)) {
                escapedPart = part.replace("]", "]]");
            } else {
                escapedPart = part.replace(cq, cq + cq);
            }

            sb.append(q).append(escapedPart).append(cq);
        }
        return sb.toString();
    }
}
