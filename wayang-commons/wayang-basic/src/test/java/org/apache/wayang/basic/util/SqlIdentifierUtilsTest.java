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

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SqlIdentifierUtils}.
 */
public class SqlIdentifierUtilsTest {

    @Test
    void testValidateIdentifierValid() {
        assertDoesNotThrow(() -> SqlIdentifierUtils.validateIdentifier("users"));
        assertDoesNotThrow(() -> SqlIdentifierUtils.validateIdentifier("_temp"));
        assertDoesNotThrow(() -> SqlIdentifierUtils.validateIdentifier("col_name"));
        assertDoesNotThrow(() -> SqlIdentifierUtils.validateIdentifier("table$1"));
        assertDoesNotThrow(() -> SqlIdentifierUtils.validateIdentifier("camelCaseColumn"));
    }

    @Test
    void testValidateIdentifierInvalid() {
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateIdentifier(null));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateIdentifier(""));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateIdentifier("   "));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateIdentifier("1table"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateIdentifier("col; DROP TABLE users;"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateIdentifier("col\"name"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateIdentifier("col'name"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateIdentifier("col name"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateIdentifier("col.name"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateIdentifier("col-name"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateIdentifier("col/*comment*/"));
    }

    @Test
    void testValidateTableNameValid() {
        assertDoesNotThrow(() -> SqlIdentifierUtils.validateTableName("users"));
        assertDoesNotThrow(() -> SqlIdentifierUtils.validateTableName("_temp"));
        assertDoesNotThrow(() -> SqlIdentifierUtils.validateTableName("schema.users"));
        assertDoesNotThrow(() -> SqlIdentifierUtils.validateTableName("catalog.schema.users"));
        assertDoesNotThrow(() -> SqlIdentifierUtils.validateTableName("cat.sch.tbl_1$"));
    }

    @Test
    void testValidateTableNameInvalid() {
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateTableName(null));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateTableName(""));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateTableName("   "));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateTableName(".users"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateTableName("users."));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateTableName("users..accounts"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateTableName("users; DROP TABLE users; --"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateTableName("users\" OR 1=1"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateTableName("users' OR 1=1"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateTableName("users name"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateTableName("123users"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateTableName("users--comment"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateTableName("users/*comment*/"));
        assertThrows(IllegalArgumentException.class, () -> SqlIdentifierUtils.validateTableName("users(id INT)"));
    }

    @Test
    void testValidateColumnNames() {
        assertDoesNotThrow(() -> SqlIdentifierUtils.validateColumnNames(null));
        assertDoesNotThrow(() -> SqlIdentifierUtils.validateColumnNames(new String[0]));
        assertDoesNotThrow(() -> SqlIdentifierUtils.validateColumnNames(new String[]{"id", "name", "created_at"}));

        assertThrows(IllegalArgumentException.class,
                () -> SqlIdentifierUtils.validateColumnNames(new String[]{"id", "name; DROP TABLE users;"}));
        assertThrows(IllegalArgumentException.class,
                () -> SqlIdentifierUtils.validateColumnNames(new String[]{"id", null}));
        assertThrows(IllegalArgumentException.class,
                () -> SqlIdentifierUtils.validateColumnNames(new String[]{"id", ""}));
        assertThrows(IllegalArgumentException.class,
                () -> SqlIdentifierUtils.validateColumnNames(new String[]{"id", "bad col"}));
    }

    @Test
    void testEscapeAndQuoteIdentifierNullConnection() {
        assertEquals("\"users\"", SqlIdentifierUtils.escapeAndQuoteIdentifier("users", (Connection) null));
        assertEquals("\"schema\".\"users\"",
                SqlIdentifierUtils.escapeAndQuoteIdentifier("schema.users", (Connection) null));
        assertEquals("\"catalog\".\"schema\".\"users\"",
                SqlIdentifierUtils.escapeAndQuoteIdentifier("catalog.schema.users", (Connection) null));
    }

    @Test
    void testEscapeAndQuoteIdentifierWithMockConnection() throws SQLException {
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(meta);

        // Standard quoting
        when(meta.getIdentifierQuoteString()).thenReturn("\"");
        when(meta.storesUpperCaseIdentifiers()).thenReturn(false);
        when(meta.storesLowerCaseIdentifiers()).thenReturn(false);
        assertEquals("\"my_table\"", SqlIdentifierUtils.escapeAndQuoteIdentifier("my_table", conn));
        assertEquals("\"my_schema\".\"my_table\"",
                SqlIdentifierUtils.escapeAndQuoteIdentifier("my_schema.my_table", conn));

        // MySQL backtick quoting
        when(meta.getIdentifierQuoteString()).thenReturn("`");
        assertEquals("`my_table`", SqlIdentifierUtils.escapeAndQuoteIdentifier("my_table", conn));
        assertEquals("`my_schema`.`my_table`",
                SqlIdentifierUtils.escapeAndQuoteIdentifier("my_schema.my_table", conn));

        // MSSQL bracket quoting
        when(meta.getIdentifierQuoteString()).thenReturn("[");
        assertEquals("[my_table]", SqlIdentifierUtils.escapeAndQuoteIdentifier("my_table", conn));
        assertEquals("[my_schema].[my_table]",
                SqlIdentifierUtils.escapeAndQuoteIdentifier("my_schema.my_table", conn));
    }

    @Test
    void testEscapeAndQuoteIdentifierWithCaseFolding() {
        // Explicit case folding
        assertEquals("\"MY_TABLE\"",
                SqlIdentifierUtils.escapeAndQuoteIdentifier("my_table", "\"", "\"", true, false));
        assertEquals("\"my_table\"",
                SqlIdentifierUtils.escapeAndQuoteIdentifier("MY_TABLE", "\"", "\"", false, true));
        assertEquals("\"my_table\"",
                SqlIdentifierUtils.escapeAndQuoteIdentifier("my_table", "\"", "\"", false, false));
    }

    @Test
    void testEscapeAndQuoteIdentifierWithDatabaseProduct() {
        assertEquals("`users`", SqlIdentifierUtils.escapeAndQuoteIdentifier("users", DatabaseProduct.MYSQL));
        assertEquals("`db`.`users`", SqlIdentifierUtils.escapeAndQuoteIdentifier("db.users", DatabaseProduct.MYSQL));

        assertEquals("[users]", SqlIdentifierUtils.escapeAndQuoteIdentifier("users", DatabaseProduct.MSSQL));
        assertEquals("[dbo].[users]", SqlIdentifierUtils.escapeAndQuoteIdentifier("dbo.users", DatabaseProduct.MSSQL));

        assertEquals("\"users\"", SqlIdentifierUtils.escapeAndQuoteIdentifier("users", DatabaseProduct.POSTGRESQL));
        assertEquals("\"users\"", SqlIdentifierUtils.escapeAndQuoteIdentifier("users", DatabaseProduct.ORACLE));
    }

    @Test
    void testEscapeAndQuoteIdentifierRejectsInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> SqlIdentifierUtils.escapeAndQuoteIdentifier("users; DROP TABLE test;", (Connection) null));
        assertThrows(IllegalArgumentException.class,
                () -> SqlIdentifierUtils.escapeAndQuoteIdentifier("users\" OR 1=1", DatabaseProduct.MYSQL));
    }
}
