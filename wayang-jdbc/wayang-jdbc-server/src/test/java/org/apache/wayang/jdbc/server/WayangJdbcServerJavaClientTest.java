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

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.wayang.api.sql.context.SqlColumn;
import org.apache.wayang.api.sql.context.SqlQueryResult;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.jdbc.driver.WayangDriver;
import org.apache.wayang.jdbc.protocol.message.ColumnInfo;
import org.apache.wayang.jdbc.protocol.message.GetColumnsRequest;
import org.apache.wayang.jdbc.protocol.message.GetSchemasRequest;
import org.apache.wayang.jdbc.protocol.message.GetTablesRequest;
import org.apache.wayang.jdbc.protocol.message.MetadataResultResponse;
import org.apache.wayang.jdbc.protocol.message.MetadataType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WayangJdbcServerJavaClientTest {

    @BeforeAll
    static void registerDriver() throws Exception {
        Class.forName(WayangDriver.class.getName());
    }

    @Test
    void driverManagerClientExecutesSelectWithPagingAndTypedGetters() throws Exception {
        final FakeQueryExecutor executor = new FakeQueryExecutor();
        try (WayangJdbcServer server = new WayangJdbcServer(
                "127.0.0.1",
                0,
                executor,
                new FakeMetadataProvider()
        )) {
            server.start();

            try (Connection connection = DriverManager.getConnection(this.url(server), "test-user", "test-password");
                 Statement statement = connection.createStatement()) {
                assertTrue(connection.isReadOnly());
                assertTrue(connection.getAutoCommit());
                assertEquals("analytics", connection.getCatalog());
                assertTrue(connection.isValid(1));

                statement.setFetchSize(2);

                try (ResultSet resultSet = statement.executeQuery("SELECT * FROM people ORDER BY ID")) {
                    final ResultSetMetaData metaData = resultSet.getMetaData();
                    assertEquals(6, metaData.getColumnCount());
                    assertEquals("ID", metaData.getColumnLabel(1));
                    assertEquals(Types.INTEGER, metaData.getColumnType(1));
                    assertEquals(Types.DECIMAL, metaData.getColumnType(3));
                    assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(3));

                    assertTrue(resultSet.next());
                    assertEquals(1, resultSet.getInt("id"));
                    assertEquals(Integer.valueOf(1), resultSet.getObject("ID", Integer.class));
                    assertEquals("alice", resultSet.getString("NAME"));
                    assertFalse(resultSet.wasNull());
                    assertEquals(0, new BigDecimal("10.50").compareTo(resultSet.getBigDecimal("AMOUNT")));
                    assertTrue(resultSet.getBoolean("ACTIVE"));
                    assertEquals(Date.valueOf(LocalDate.of(2026, 7, 27)), resultSet.getObject("CREATED_ON"));
                    assertArrayEquals(new byte[]{1, 2, 3}, resultSet.getBytes("PAYLOAD"));

                    assertTrue(resultSet.next());
                    assertEquals(2, resultSet.getInt(1));
                    assertEquals("bob", resultSet.getString(2));

                    assertTrue(resultSet.next());
                    assertEquals(3, resultSet.getInt(1));
                    assertNullValueWasReported(resultSet);

                    assertTrue(resultSet.next());
                    assertEquals(4, resultSet.getInt(1));

                    assertTrue(resultSet.next());
                    assertEquals(5, resultSet.getInt(1));
                    assertFalse(resultSet.next());
                }
            }
        }

        assertEquals(List.of("SELECT * FROM people ORDER BY ID"), executor.getExecutedSql());
    }

    @Test
    void javaClientRejectsWritesBeforeExecution() throws Exception {
        final FakeQueryExecutor executor = new FakeQueryExecutor();
        try (WayangJdbcServer server = new WayangJdbcServer(
                "127.0.0.1",
                0,
                executor,
                new FakeMetadataProvider()
        )) {
            server.start();

            try (Connection connection = DriverManager.getConnection(this.url(server));
                 Statement statement = connection.createStatement()) {
                final SQLFeatureNotSupportedException exception = assertThrows(
                        SQLFeatureNotSupportedException.class,
                        () -> statement.executeQuery("INSERT INTO people VALUES (1, 'blocked')")
                );

                assertEquals("0A000", exception.getSQLState());
                assertEquals(Collections.emptyList(), executor.getExecutedSql());
            }
        }
    }

    @Test
    void javaClientBrowsesDatabaseMetadata() throws Exception {
        final FakeMetadataProvider metadataProvider = new FakeMetadataProvider();
        try (WayangJdbcServer server = new WayangJdbcServer(
                "127.0.0.1",
                0,
                new FakeQueryExecutor(),
                metadataProvider
        )) {
            server.start();

            try (Connection connection = DriverManager.getConnection(this.url(server));
                 ResultSet schemas = connection.getMetaData().getSchemas("analytics", "pub%");
                 ResultSet tables = connection.getMetaData().getTables(
                         "analytics",
                         "public",
                         "PEOPLE",
                         new String[]{"TABLE"}
                 );
                 ResultSet columns = connection.getMetaData().getColumns(
                         "analytics",
                         "public",
                         "PEOPLE",
                         "%"
                 )) {
                final DatabaseMetaData metaData = connection.getMetaData();
                assertTrue(metaData.isReadOnly());
                assertTrue(metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
                assertTrue(metaData.supportsResultSetConcurrency(
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY
                ));
                assertFalse(metaData.supportsTransactions());

                assertTrue(schemas.next());
                assertEquals("public", schemas.getString("TABLE_SCHEM"));
                assertEquals("analytics", schemas.getString("TABLE_CATALOG"));
                assertFalse(schemas.next());

                assertTrue(tables.next());
                assertEquals("analytics", tables.getString("TABLE_CAT"));
                assertEquals("public", tables.getString("TABLE_SCHEM"));
                assertEquals("PEOPLE", tables.getString("TABLE_NAME"));
                assertEquals("TABLE", tables.getString("TABLE_TYPE"));
                assertFalse(tables.next());

                assertTrue(columns.next());
                assertEquals("ID", columns.getString("COLUMN_NAME"));
                assertEquals(Types.INTEGER, columns.getInt("DATA_TYPE"));
                assertEquals(1, columns.getInt("ORDINAL_POSITION"));

                assertTrue(columns.next());
                assertEquals("NAME", columns.getString("COLUMN_NAME"));
                assertEquals(Types.VARCHAR, columns.getInt("DATA_TYPE"));
                assertEquals(2, columns.getInt("ORDINAL_POSITION"));
                assertFalse(columns.next());
            }
        }

        assertEquals("analytics", metadataProvider.getLastSchemasRequest().getCatalog());
        assertEquals("pub%", metadataProvider.getLastSchemasRequest().getSchemaPattern());
        assertEquals("PEOPLE", metadataProvider.getLastTablesRequest().getTableNamePattern());
        assertArrayEquals(
                new String[]{"TABLE"},
                metadataProvider.getLastTablesRequest().getTableTypes().toArray(new String[0])
        );
        assertEquals("%", metadataProvider.getLastColumnsRequest().getColumnNamePattern());
    }

    @Test
    void isValidUsesServerPing() throws Exception {
        final Connection connection;
        final WayangJdbcServer server = new WayangJdbcServer(
                "127.0.0.1",
                0,
                new FakeQueryExecutor(),
                new FakeMetadataProvider()
        );
        server.start();
        connection = DriverManager.getConnection(this.url(server));
        assertTrue(connection.isValid(1));

        server.close();
        assertFalse(connection.isValid(1));
        connection.close();
    }

    private String url(final WayangJdbcServer server) {
        return "jdbc:wayang://127.0.0.1:" + server.getPort() + "/analytics?connectTimeout=5000";
    }

    private static void assertNullValueWasReported(final ResultSet resultSet) throws SQLException {
        assertEquals(0, resultSet.getInt("AMOUNT"));
        assertTrue(resultSet.wasNull());
        assertEquals(null, resultSet.getString("PAYLOAD"));
        assertTrue(resultSet.wasNull());
    }

    private static ColumnInfo column(final String name, final int jdbcType) {
        return new ColumnInfo(
                name,
                name,
                null,
                null,
                jdbcTypeName(jdbcType),
                jdbcType,
                ResultSetMetaData.columnNullable,
                64,
                0
        );
    }

    private static String jdbcTypeName(final int jdbcType) {
        switch (jdbcType) {
            case Types.INTEGER:
                return "INTEGER";
            case Types.DECIMAL:
                return "DECIMAL";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.DATE:
                return "DATE";
            case Types.VARBINARY:
                return "VARBINARY";
            default:
                return "VARCHAR";
        }
    }

    private static class FakeQueryExecutor implements SqlQueryExecutor {

        private final List<String> executedSql = new ArrayList<>();

        @Override
        public SqlQueryResult execute(final String sql) throws SqlParseException {
            this.executedSql.add(sql);
            return new SqlQueryResult(
                    Arrays.asList(
                            new SqlColumn("ID", "ID", "INTEGER", Types.INTEGER, 10, 0, false),
                            new SqlColumn("NAME", "NAME", "VARCHAR", Types.VARCHAR, 64, 0, true),
                            new SqlColumn("AMOUNT", "AMOUNT", "DECIMAL", Types.DECIMAL, 10, 2, true),
                            new SqlColumn("ACTIVE", "ACTIVE", "BOOLEAN", Types.BOOLEAN, 1, 0, false),
                            new SqlColumn("CREATED_ON", "CREATED_ON", "DATE", Types.DATE, 10, 0, true),
                            new SqlColumn("PAYLOAD", "PAYLOAD", "VARBINARY", Types.VARBINARY, 64, 0, true)
                    ),
                    Arrays.asList(
                            new Record(
                                    1,
                                    "alice",
                                    new BigDecimal("10.50"),
                                    true,
                                    LocalDate.of(2026, 7, 27),
                                    new byte[]{1, 2, 3}
                            ),
                            new Record(
                                    2,
                                    "bob",
                                    new BigDecimal("20.00"),
                                    false,
                                    LocalDate.of(2026, 7, 28),
                                    new byte[]{4, 5, 6}
                            ),
                            new Record(3, "charlie", null, true, null, null),
                            new Record(
                                    4,
                                    "dana",
                                    new BigDecimal("40.25"),
                                    false,
                                    LocalDate.of(2026, 7, 29),
                                    new byte[]{7}
                            ),
                            new Record(
                                    5,
                                    "erin",
                                    new BigDecimal("50.75"),
                                    true,
                                    LocalDate.of(2026, 7, 30),
                                    new byte[]{8, 9}
                            )
                    )
            );
        }

        List<String> getExecutedSql() {
            return List.copyOf(this.executedSql);
        }
    }

    private static class FakeMetadataProvider implements SqlMetadataProvider {

        private GetSchemasRequest lastSchemasRequest;

        private GetTablesRequest lastTablesRequest;

        private GetColumnsRequest lastColumnsRequest;

        @Override
        public MetadataResultResponse getSchemas(
                final JdbcServerSession session,
                final GetSchemasRequest request
        ) {
            this.lastSchemasRequest = request;
            return new MetadataResultResponse(
                    session.getConnectionId(),
                    MetadataType.SCHEMAS,
                    Arrays.asList(column("TABLE_SCHEM", Types.VARCHAR), column("TABLE_CATALOG", Types.VARCHAR)),
                    Collections.singletonList(Arrays.asList("public", "analytics"))
            );
        }

        @Override
        public MetadataResultResponse getTables(
                final JdbcServerSession session,
                final GetTablesRequest request
        ) {
            this.lastTablesRequest = request;
            return new MetadataResultResponse(
                    session.getConnectionId(),
                    MetadataType.TABLES,
                    Arrays.asList(
                            column("TABLE_CAT", Types.VARCHAR),
                            column("TABLE_SCHEM", Types.VARCHAR),
                            column("TABLE_NAME", Types.VARCHAR),
                            column("TABLE_TYPE", Types.VARCHAR),
                            column("REMARKS", Types.VARCHAR),
                            column("TYPE_CAT", Types.VARCHAR),
                            column("TYPE_SCHEM", Types.VARCHAR),
                            column("TYPE_NAME", Types.VARCHAR),
                            column("SELF_REFERENCING_COL_NAME", Types.VARCHAR),
                            column("REF_GENERATION", Types.VARCHAR)
                    ),
                    Collections.singletonList(Arrays.asList(
                            "analytics",
                            "public",
                            "PEOPLE",
                            "TABLE",
                            null,
                            null,
                            null,
                            null,
                            null,
                            null
                    ))
            );
        }

        @Override
        public MetadataResultResponse getColumns(
                final JdbcServerSession session,
                final GetColumnsRequest request
        ) {
            this.lastColumnsRequest = request;
            return new MetadataResultResponse(
                    session.getConnectionId(),
                    MetadataType.COLUMNS,
                    Arrays.asList(
                            column("TABLE_CAT", Types.VARCHAR),
                            column("TABLE_SCHEM", Types.VARCHAR),
                            column("TABLE_NAME", Types.VARCHAR),
                            column("COLUMN_NAME", Types.VARCHAR),
                            column("DATA_TYPE", Types.INTEGER),
                            column("TYPE_NAME", Types.VARCHAR),
                            column("COLUMN_SIZE", Types.INTEGER),
                            column("BUFFER_LENGTH", Types.INTEGER),
                            column("DECIMAL_DIGITS", Types.INTEGER),
                            column("NUM_PREC_RADIX", Types.INTEGER),
                            column("NULLABLE", Types.INTEGER),
                            column("REMARKS", Types.VARCHAR),
                            column("COLUMN_DEF", Types.VARCHAR),
                            column("SQL_DATA_TYPE", Types.INTEGER),
                            column("SQL_DATETIME_SUB", Types.INTEGER),
                            column("CHAR_OCTET_LENGTH", Types.INTEGER),
                            column("ORDINAL_POSITION", Types.INTEGER),
                            column("IS_NULLABLE", Types.VARCHAR),
                            column("SCOPE_CATALOG", Types.VARCHAR),
                            column("SCOPE_SCHEMA", Types.VARCHAR),
                            column("SCOPE_TABLE", Types.VARCHAR),
                            column("SOURCE_DATA_TYPE", Types.SMALLINT),
                            column("IS_AUTOINCREMENT", Types.VARCHAR),
                            column("IS_GENERATEDCOLUMN", Types.VARCHAR)
                    ),
                    Arrays.asList(
                            columnRow("ID", Types.INTEGER, "INTEGER", 10, 0, 1, "NO"),
                            columnRow("NAME", Types.VARCHAR, "VARCHAR", 64, 0, 2, "YES")
                    )
            );
        }

        GetSchemasRequest getLastSchemasRequest() {
            assertNotNull(this.lastSchemasRequest);
            return this.lastSchemasRequest;
        }

        GetTablesRequest getLastTablesRequest() {
            assertNotNull(this.lastTablesRequest);
            return this.lastTablesRequest;
        }

        GetColumnsRequest getLastColumnsRequest() {
            assertNotNull(this.lastColumnsRequest);
            return this.lastColumnsRequest;
        }

        private static List<Object> columnRow(
                final String columnName,
                final int jdbcType,
                final String typeName,
                final int precision,
                final int scale,
                final int ordinal,
                final String nullable
        ) {
            return Arrays.asList(
                    "analytics",
                    "public",
                    "PEOPLE",
                    columnName,
                    jdbcType,
                    typeName,
                    precision,
                    null,
                    scale,
                    jdbcType == Types.INTEGER ? 10 : null,
                    "YES".equals(nullable)
                            ? DatabaseMetaData.columnNullable
                            : DatabaseMetaData.columnNoNulls,
                    null,
                    null,
                    null,
                    null,
                    jdbcType == Types.VARCHAR ? precision : null,
                    ordinal,
                    nullable,
                    null,
                    null,
                    null,
                    null,
                    "NO",
                    "NO"
            );
        }
    }
}
