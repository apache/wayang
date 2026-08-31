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
import org.apache.wayang.jdbc.protocol.message.MetadataResultResponse;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

final class WayangDatabaseMetaData implements InvocationHandler {

    private static final String DRIVER_NAME = "Apache Wayang JDBC Driver";

    private static final String PRODUCT_NAME = "Apache Wayang";

    private static final String FALLBACK_VERSION = "1.1.2-SNAPSHOT";

    private final WayangConnection connection;

    private WayangDatabaseMetaData(final WayangConnection connection) {
        if (connection == null) {
            throw new IllegalArgumentException("Connection must not be null.");
        }
        this.connection = connection;
    }

    static DatabaseMetaData create(final WayangConnection connection) {
        return (DatabaseMetaData) Proxy.newProxyInstance(
                DatabaseMetaData.class.getClassLoader(),
                new Class<?>[]{DatabaseMetaData.class},
                new WayangDatabaseMetaData(connection)
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

        this.ensureOpen();

        final String methodName = method.getName();
        switch (methodName) {
            case "getConnection":
                return this.connection;
            case "getURL":
                return this.emptyIfNull(this.connection.getJdbcUrl());
            case "getUserName":
                return this.emptyIfNull(this.connection.getUserName());
            case "getDriverName":
                return DRIVER_NAME;
            case "getDriverVersion":
                return this.version();
            case "getDriverMajorVersion":
                return 1;
            case "getDriverMinorVersion":
                return 0;
            case "getDatabaseProductName":
                return PRODUCT_NAME;
            case "getDatabaseProductVersion":
                return this.databaseVersion();
            case "getDatabaseMajorVersion":
                return this.databaseVersionPart(0, 1);
            case "getDatabaseMinorVersion":
                return this.databaseVersionPart(1, 1);
            case "getJDBCMajorVersion":
                return 4;
            case "getJDBCMinorVersion":
                return 2;
            case "isReadOnly":
                return true;
            case "supportsMixedCaseIdentifiers":
            case "storesMixedCaseIdentifiers":
            case "supportsMixedCaseQuotedIdentifiers":
            case "storesMixedCaseQuotedIdentifiers":
                return true;
            case "storesUpperCaseIdentifiers":
            case "storesLowerCaseIdentifiers":
            case "storesUpperCaseQuotedIdentifiers":
            case "storesLowerCaseQuotedIdentifiers":
                return false;
            case "allProceduresAreCallable":
                return false;
            case "allTablesAreSelectable":
                return true;
            case "getSQLStateType":
                return DatabaseMetaData.sqlStateSQL;
            case "getResultSetHoldability":
                return ResultSet.CLOSE_CURSORS_AT_COMMIT;
            case "supportsSchemasInDataManipulation":
                return true;
            case "supportsSchemasInProcedureCalls":
            case "supportsSchemasInTableDefinitions":
            case "supportsSchemasInIndexDefinitions":
            case "supportsSchemasInPrivilegeDefinitions":
            case "supportsCatalogsInDataManipulation":
            case "supportsCatalogsInProcedureCalls":
            case "supportsCatalogsInTableDefinitions":
            case "supportsCatalogsInIndexDefinitions":
            case "supportsCatalogsInPrivilegeDefinitions":
                return false;
            case "supportsResultSetType":
                return (Integer) args[0] == ResultSet.TYPE_FORWARD_ONLY;
            case "supportsResultSetConcurrency":
                return (Integer) args[0] == ResultSet.TYPE_FORWARD_ONLY
                        && (Integer) args[1] == ResultSet.CONCUR_READ_ONLY;
            case "supportsResultSetHoldability":
                return (Integer) args[0] == ResultSet.CLOSE_CURSORS_AT_COMMIT;
            case "supportsTransactions":
            case "supportsMultipleTransactions":
            case "supportsSavepoints":
            case "supportsBatchUpdates":
            case "supportsStoredProcedures":
            case "supportsNamedParameters":
            case "supportsGetGeneratedKeys":
            case "supportsMultipleOpenResults":
            case "supportsMultipleResultSets":
            case "supportsStatementPooling":
            case "supportsDataDefinitionAndDataManipulationTransactions":
            case "supportsDataManipulationTransactionsOnly":
            case "dataDefinitionCausesTransactionCommit":
            case "dataDefinitionIgnoredInTransactions":
            case "supportsOpenCursorsAcrossCommit":
            case "supportsOpenCursorsAcrossRollback":
            case "supportsOpenStatementsAcrossCommit":
            case "supportsOpenStatementsAcrossRollback":
            case "supportsStoredFunctionsUsingCallSyntax":
            case "autoCommitFailureClosesAllResultSets":
            case "generatedKeyAlwaysReturned":
            case "supportsRefCursors":
            case "supportsSharding":
                return false;
            case "getDefaultTransactionIsolation":
                return Connection.TRANSACTION_NONE;
            case "getIdentifierQuoteString":
                return "\"";
            case "getSearchStringEscape":
                return "\\";
            case "getExtraNameCharacters":
            case "getSQLKeywords":
            case "getNumericFunctions":
            case "getStringFunctions":
            case "getSystemFunctions":
            case "getTimeDateFunctions":
                return "";
            case "getCatalogTerm":
                return "catalog";
            case "getSchemaTerm":
                return "schema";
            case "getProcedureTerm":
                return "procedure";
            case "getCatalogSeparator":
                return ".";
            case "isCatalogAtStart":
                return true;
            case "nullsAreSortedHigh":
                return true;
            case "nullsAreSortedLow":
            case "nullsAreSortedAtStart":
            case "nullsAreSortedAtEnd":
                return false;
            case "getCatalogs":
                return this.catalogs();
            case "getSchemas":
                return this.schemas(args);
            case "getTableTypes":
                return this.tableTypes();
            case "getTables":
                return this.tables(args);
            case "getColumns":
                return this.columns(args);
            case "getProcedures":
                return this.procedures();
            case "getProcedureColumns":
                return this.procedureColumns();
            case "getTypeInfo":
                return this.typeInfo();
            case "getColumnPrivileges":
                return this.columnPrivileges();
            case "getTablePrivileges":
                return this.tablePrivileges();
            case "getBestRowIdentifier":
                return this.bestRowIdentifier();
            case "getVersionColumns":
                return this.versionColumns();
            case "getImportedKeys":
            case "getExportedKeys":
            case "getCrossReference":
                return this.foreignKeys();
            case "getPrimaryKeys":
                return this.primaryKeys();
            case "getIndexInfo":
                return this.indexInfo();
            case "getUDTs":
                return this.udts();
            case "getSuperTypes":
                return this.superTypes();
            case "getSuperTables":
                return this.superTables();
            case "getAttributes":
                return this.attributes();
            case "getClientInfoProperties":
                return this.clientInfoProperties();
            case "getFunctions":
                return this.functions();
            case "getFunctionColumns":
                return this.functionColumns();
            case "getPseudoColumns":
                return this.pseudoColumns();
            case "getRowIdLifetime":
                return RowIdLifetime.ROWID_UNSUPPORTED;
            case "unwrap":
                return this.unwrap(proxy, (Class<?>) args[0]);
            case "isWrapperFor":
                return this.isWrapperFor(proxy, (Class<?>) args[0]);
            default:
                return this.defaultValue(method);
        }
    }

    private Object invokeObjectMethod(
            final Object proxy,
            final Method method,
            final Object[] args
    ) {
        switch (method.getName()) {
            case "toString":
                return "WayangDatabaseMetaData";
            case "hashCode":
                return System.identityHashCode(proxy);
            case "equals":
                return proxy == args[0];
            default:
                throw new IllegalStateException("Unexpected Object method: " + method.getName());
        }
    }

    private ResultSet catalogs() throws SQLException {
        final String catalog = this.connection.getCatalog();
        final List<List<Object>> rows = new ArrayList<>();
        if (catalog != null) {
            rows.add(Collections.singletonList(catalog));
        }
        return this.resultSet(
                Arrays.asList(column("TABLE_CAT", Types.VARCHAR)),
                rows
        );
    }

    private ResultSet schemas(final Object[] args) throws SQLException {
        final String catalog = this.stringArg(args, 0);
        final String schemaPattern = this.stringArg(args, 1);
        return this.resultSet(this.connection.getClient().getSchemas(catalog, schemaPattern));
    }

    private ResultSet tableTypes() throws SQLException {
        return this.resultSet(
                Arrays.asList(column("TABLE_TYPE", Types.VARCHAR)),
                Arrays.asList(
                        Collections.singletonList("TABLE"),
                        Collections.singletonList("VIEW")
                )
        );
    }

    private ResultSet tables(final Object[] args) throws SQLException {
        return this.resultSet(this.connection.getClient().getTables(
                this.stringArg(args, 0),
                this.stringArg(args, 1),
                this.stringArg(args, 2),
                this.stringArrayArg(args, 3)
        ));
    }

    private ResultSet columns(final Object[] args) throws SQLException {
        return this.resultSet(this.connection.getClient().getColumns(
                this.stringArg(args, 0),
                this.stringArg(args, 1),
                this.stringArg(args, 2),
                this.stringArg(args, 3)
        ));
    }

    private ResultSet procedures() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("PROCEDURE_CAT", Types.VARCHAR),
                        column("PROCEDURE_SCHEM", Types.VARCHAR),
                        column("PROCEDURE_NAME", Types.VARCHAR),
                        column("RESERVED1", Types.VARCHAR),
                        column("RESERVED2", Types.VARCHAR),
                        column("RESERVED3", Types.VARCHAR),
                        column("REMARKS", Types.VARCHAR),
                        column("PROCEDURE_TYPE", Types.SMALLINT),
                        column("SPECIFIC_NAME", Types.VARCHAR)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet procedureColumns() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("PROCEDURE_CAT", Types.VARCHAR),
                        column("PROCEDURE_SCHEM", Types.VARCHAR),
                        column("PROCEDURE_NAME", Types.VARCHAR),
                        column("COLUMN_NAME", Types.VARCHAR),
                        column("COLUMN_TYPE", Types.SMALLINT),
                        column("DATA_TYPE", Types.INTEGER),
                        column("TYPE_NAME", Types.VARCHAR),
                        column("PRECISION", Types.INTEGER),
                        column("LENGTH", Types.INTEGER),
                        column("SCALE", Types.SMALLINT),
                        column("RADIX", Types.SMALLINT),
                        column("NULLABLE", Types.SMALLINT),
                        column("REMARKS", Types.VARCHAR),
                        column("COLUMN_DEF", Types.VARCHAR),
                        column("SQL_DATA_TYPE", Types.INTEGER),
                        column("SQL_DATETIME_SUB", Types.INTEGER),
                        column("CHAR_OCTET_LENGTH", Types.INTEGER),
                        column("ORDINAL_POSITION", Types.INTEGER),
                        column("IS_NULLABLE", Types.VARCHAR),
                        column("SPECIFIC_NAME", Types.VARCHAR)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet typeInfo() throws SQLException {
        final List<MetadataColumn> columns = Arrays.asList(
                column("TYPE_NAME", Types.VARCHAR),
                column("DATA_TYPE", Types.INTEGER),
                column("PRECISION", Types.INTEGER),
                column("LITERAL_PREFIX", Types.VARCHAR),
                column("LITERAL_SUFFIX", Types.VARCHAR),
                column("CREATE_PARAMS", Types.VARCHAR),
                column("NULLABLE", Types.SMALLINT),
                column("CASE_SENSITIVE", Types.BOOLEAN),
                column("SEARCHABLE", Types.SMALLINT),
                column("UNSIGNED_ATTRIBUTE", Types.BOOLEAN),
                column("FIXED_PREC_SCALE", Types.BOOLEAN),
                column("AUTO_INCREMENT", Types.BOOLEAN),
                column("LOCAL_TYPE_NAME", Types.VARCHAR),
                column("MINIMUM_SCALE", Types.SMALLINT),
                column("MAXIMUM_SCALE", Types.SMALLINT),
                column("SQL_DATA_TYPE", Types.INTEGER),
                column("SQL_DATETIME_SUB", Types.INTEGER),
                column("NUM_PREC_RADIX", Types.INTEGER)
        );
        final List<List<Object>> rows = new ArrayList<>();
        rows.add(this.typeInfoRow("BOOLEAN", Types.BOOLEAN, 1, null, null, null, false, 0, 0, null));
        rows.add(this.typeInfoRow("TINYINT", Types.TINYINT, 3, null, null, null, false, 0, 0, 10));
        rows.add(this.typeInfoRow("SMALLINT", Types.SMALLINT, 5, null, null, null, false, 0, 0, 10));
        rows.add(this.typeInfoRow("INTEGER", Types.INTEGER, 10, null, null, null, false, 0, 0, 10));
        rows.add(this.typeInfoRow("BIGINT", Types.BIGINT, 19, null, null, null, false, 0, 0, 10));
        rows.add(this.typeInfoRow(
                "NUMERIC", Types.NUMERIC, 38, null, null, "precision,scale", false, 0, 38, 10
        ));
        rows.add(this.typeInfoRow(
                "DECIMAL", Types.DECIMAL, 38, null, null, "precision,scale", false, 0, 38, 10
        ));
        rows.add(this.typeInfoRow("REAL", Types.REAL, 7, null, null, null, false, 0, 0, 2));
        rows.add(this.typeInfoRow("FLOAT", Types.FLOAT, 15, null, null, null, false, 0, 0, 2));
        rows.add(this.typeInfoRow("DOUBLE", Types.DOUBLE, 15, null, null, null, false, 0, 0, 2));
        rows.add(this.typeInfoRow("CHAR", Types.CHAR, 65536, "'", "'", "length", true, 0, 0, null));
        rows.add(this.typeInfoRow("VARCHAR", Types.VARCHAR, 65536, "'", "'", "length", true, 0, 0, null));
        rows.add(this.typeInfoRow("BINARY", Types.BINARY, 65536, "X'", "'", "length", false, 0, 0, null));
        rows.add(this.typeInfoRow(
                "VARBINARY", Types.VARBINARY, 65536, "X'", "'", "length", false, 0, 0, null
        ));
        rows.add(this.typeInfoRow("DATE", Types.DATE, 10, "DATE '", "'", null, false, 0, 0, null));
        rows.add(this.typeInfoRow("TIME", Types.TIME, 18, "TIME '", "'", "precision", false, 0, 9, null));
        rows.add(this.typeInfoRow(
                "TIME WITH TIME ZONE",
                Types.TIME_WITH_TIMEZONE,
                24,
                "TIME '",
                "'",
                "precision",
                false,
                0,
                9,
                null
        ));
        rows.add(this.typeInfoRow(
                "TIMESTAMP", Types.TIMESTAMP, 29, "TIMESTAMP '", "'", "precision", false, 0, 9, null
        ));
        rows.add(this.typeInfoRow(
                "TIMESTAMP WITH TIME ZONE",
                Types.TIMESTAMP_WITH_TIMEZONE,
                35,
                "TIMESTAMP '",
                "'",
                "precision",
                false,
                0,
                9,
                null
        ));
        rows.sort(Comparator.comparingInt(row -> (Integer) row.get(1)));
        return this.resultSet(columns, rows);
    }

    private List<Object> typeInfoRow(
            final String name,
            final int jdbcType,
            final int precision,
            final String literalPrefix,
            final String literalSuffix,
            final String createParameters,
            final boolean caseSensitive,
            final int minimumScale,
            final int maximumScale,
            final Integer numericRadix
    ) {
        return Arrays.asList(
                name,
                jdbcType,
                precision,
                literalPrefix,
                literalSuffix,
                createParameters,
                (short) DatabaseMetaData.typeNullable,
                caseSensitive,
                (short) DatabaseMetaData.typeSearchable,
                null,
                false,
                false,
                null,
                (short) minimumScale,
                (short) maximumScale,
                null,
                null,
                numericRadix
        );
    }

    private ResultSet columnPrivileges() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("TABLE_CAT", Types.VARCHAR),
                        column("TABLE_SCHEM", Types.VARCHAR),
                        column("TABLE_NAME", Types.VARCHAR),
                        column("COLUMN_NAME", Types.VARCHAR),
                        column("GRANTOR", Types.VARCHAR),
                        column("GRANTEE", Types.VARCHAR),
                        column("PRIVILEGE", Types.VARCHAR),
                        column("IS_GRANTABLE", Types.VARCHAR)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet tablePrivileges() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("TABLE_CAT", Types.VARCHAR),
                        column("TABLE_SCHEM", Types.VARCHAR),
                        column("TABLE_NAME", Types.VARCHAR),
                        column("GRANTOR", Types.VARCHAR),
                        column("GRANTEE", Types.VARCHAR),
                        column("PRIVILEGE", Types.VARCHAR),
                        column("IS_GRANTABLE", Types.VARCHAR)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet bestRowIdentifier() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("SCOPE", Types.SMALLINT),
                        column("COLUMN_NAME", Types.VARCHAR),
                        column("DATA_TYPE", Types.INTEGER),
                        column("TYPE_NAME", Types.VARCHAR),
                        column("COLUMN_SIZE", Types.INTEGER),
                        column("BUFFER_LENGTH", Types.INTEGER),
                        column("DECIMAL_DIGITS", Types.SMALLINT),
                        column("PSEUDO_COLUMN", Types.SMALLINT)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet versionColumns() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("SCOPE", Types.SMALLINT),
                        column("COLUMN_NAME", Types.VARCHAR),
                        column("DATA_TYPE", Types.INTEGER),
                        column("TYPE_NAME", Types.VARCHAR),
                        column("COLUMN_SIZE", Types.INTEGER),
                        column("BUFFER_LENGTH", Types.INTEGER),
                        column("DECIMAL_DIGITS", Types.SMALLINT),
                        column("PSEUDO_COLUMN", Types.SMALLINT)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet primaryKeys() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("TABLE_CAT", Types.VARCHAR),
                        column("TABLE_SCHEM", Types.VARCHAR),
                        column("TABLE_NAME", Types.VARCHAR),
                        column("COLUMN_NAME", Types.VARCHAR),
                        column("KEY_SEQ", Types.SMALLINT),
                        column("PK_NAME", Types.VARCHAR)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet foreignKeys() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("PKTABLE_CAT", Types.VARCHAR),
                        column("PKTABLE_SCHEM", Types.VARCHAR),
                        column("PKTABLE_NAME", Types.VARCHAR),
                        column("PKCOLUMN_NAME", Types.VARCHAR),
                        column("FKTABLE_CAT", Types.VARCHAR),
                        column("FKTABLE_SCHEM", Types.VARCHAR),
                        column("FKTABLE_NAME", Types.VARCHAR),
                        column("FKCOLUMN_NAME", Types.VARCHAR),
                        column("KEY_SEQ", Types.SMALLINT),
                        column("UPDATE_RULE", Types.SMALLINT),
                        column("DELETE_RULE", Types.SMALLINT),
                        column("FK_NAME", Types.VARCHAR),
                        column("PK_NAME", Types.VARCHAR),
                        column("DEFERRABILITY", Types.SMALLINT)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet indexInfo() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("TABLE_CAT", Types.VARCHAR),
                        column("TABLE_SCHEM", Types.VARCHAR),
                        column("TABLE_NAME", Types.VARCHAR),
                        column("NON_UNIQUE", Types.BOOLEAN),
                        column("INDEX_QUALIFIER", Types.VARCHAR),
                        column("INDEX_NAME", Types.VARCHAR),
                        column("TYPE", Types.SMALLINT),
                        column("ORDINAL_POSITION", Types.SMALLINT),
                        column("COLUMN_NAME", Types.VARCHAR),
                        column("ASC_OR_DESC", Types.VARCHAR),
                        column("CARDINALITY", Types.BIGINT),
                        column("PAGES", Types.BIGINT),
                        column("FILTER_CONDITION", Types.VARCHAR)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet udts() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("TYPE_CAT", Types.VARCHAR),
                        column("TYPE_SCHEM", Types.VARCHAR),
                        column("TYPE_NAME", Types.VARCHAR),
                        column("CLASS_NAME", Types.VARCHAR),
                        column("DATA_TYPE", Types.INTEGER),
                        column("REMARKS", Types.VARCHAR),
                        column("BASE_TYPE", Types.SMALLINT)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet superTypes() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("TYPE_CAT", Types.VARCHAR),
                        column("TYPE_SCHEM", Types.VARCHAR),
                        column("TYPE_NAME", Types.VARCHAR),
                        column("SUPERTYPE_CAT", Types.VARCHAR),
                        column("SUPERTYPE_SCHEM", Types.VARCHAR),
                        column("SUPERTYPE_NAME", Types.VARCHAR)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet superTables() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("TABLE_CAT", Types.VARCHAR),
                        column("TABLE_SCHEM", Types.VARCHAR),
                        column("TABLE_NAME", Types.VARCHAR),
                        column("SUPERTABLE_NAME", Types.VARCHAR)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet attributes() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("TYPE_CAT", Types.VARCHAR),
                        column("TYPE_SCHEM", Types.VARCHAR),
                        column("TYPE_NAME", Types.VARCHAR),
                        column("ATTR_NAME", Types.VARCHAR),
                        column("DATA_TYPE", Types.INTEGER),
                        column("ATTR_TYPE_NAME", Types.VARCHAR),
                        column("ATTR_SIZE", Types.INTEGER),
                        column("DECIMAL_DIGITS", Types.INTEGER),
                        column("NUM_PREC_RADIX", Types.INTEGER),
                        column("NULLABLE", Types.INTEGER),
                        column("REMARKS", Types.VARCHAR),
                        column("ATTR_DEF", Types.VARCHAR),
                        column("SQL_DATA_TYPE", Types.INTEGER),
                        column("SQL_DATETIME_SUB", Types.INTEGER),
                        column("CHAR_OCTET_LENGTH", Types.INTEGER),
                        column("ORDINAL_POSITION", Types.INTEGER),
                        column("IS_NULLABLE", Types.VARCHAR),
                        column("SCOPE_CATALOG", Types.VARCHAR),
                        column("SCOPE_SCHEMA", Types.VARCHAR),
                        column("SCOPE_TABLE", Types.VARCHAR),
                        column("SOURCE_DATA_TYPE", Types.SMALLINT)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet clientInfoProperties() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("NAME", Types.VARCHAR),
                        column("MAX_LEN", Types.INTEGER),
                        column("DEFAULT_VALUE", Types.VARCHAR),
                        column("DESCRIPTION", Types.VARCHAR)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet functions() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("FUNCTION_CAT", Types.VARCHAR),
                        column("FUNCTION_SCHEM", Types.VARCHAR),
                        column("FUNCTION_NAME", Types.VARCHAR),
                        column("REMARKS", Types.VARCHAR),
                        column("FUNCTION_TYPE", Types.SMALLINT),
                        column("SPECIFIC_NAME", Types.VARCHAR)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet functionColumns() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("FUNCTION_CAT", Types.VARCHAR),
                        column("FUNCTION_SCHEM", Types.VARCHAR),
                        column("FUNCTION_NAME", Types.VARCHAR),
                        column("COLUMN_NAME", Types.VARCHAR),
                        column("COLUMN_TYPE", Types.SMALLINT),
                        column("DATA_TYPE", Types.INTEGER),
                        column("TYPE_NAME", Types.VARCHAR),
                        column("PRECISION", Types.INTEGER),
                        column("LENGTH", Types.INTEGER),
                        column("SCALE", Types.SMALLINT),
                        column("RADIX", Types.SMALLINT),
                        column("NULLABLE", Types.SMALLINT),
                        column("REMARKS", Types.VARCHAR),
                        column("CHAR_OCTET_LENGTH", Types.INTEGER),
                        column("ORDINAL_POSITION", Types.INTEGER),
                        column("IS_NULLABLE", Types.VARCHAR),
                        column("SPECIFIC_NAME", Types.VARCHAR)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet pseudoColumns() throws SQLException {
        return this.resultSet(
                Arrays.asList(
                        column("TABLE_CAT", Types.VARCHAR),
                        column("TABLE_SCHEM", Types.VARCHAR),
                        column("TABLE_NAME", Types.VARCHAR),
                        column("COLUMN_NAME", Types.VARCHAR),
                        column("DATA_TYPE", Types.INTEGER),
                        column("COLUMN_SIZE", Types.INTEGER),
                        column("DECIMAL_DIGITS", Types.INTEGER),
                        column("NUM_PREC_RADIX", Types.INTEGER),
                        column("COLUMN_USAGE", Types.VARCHAR),
                        column("REMARKS", Types.VARCHAR),
                        column("CHAR_OCTET_LENGTH", Types.INTEGER),
                        column("IS_NULLABLE", Types.VARCHAR)
                ),
                Collections.emptyList()
        );
    }

    private ResultSet resultSet(
            final List<MetadataColumn> columns,
            final List<List<Object>> rows
    ) throws SQLException {
        final CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
        final RowSetMetaDataImpl metaData = new RowSetMetaDataImpl();
        metaData.setColumnCount(columns.size());

        for (int index = 0; index < columns.size(); index++) {
            final MetadataColumn column = columns.get(index);
            final int columnIndex = index + 1;
            metaData.setColumnName(columnIndex, column.name);
            metaData.setColumnLabel(columnIndex, column.name);
            metaData.setColumnType(columnIndex, column.type);
            metaData.setColumnTypeName(columnIndex, column.typeName);
            metaData.setNullable(columnIndex, ResultSetMetaData.columnNullable);
            metaData.setColumnDisplaySize(columnIndex, 128);
        }

        rowSet.setMetaData(metaData);
        // CachedRowSet.insertRow() inserts before its current cursor position.
        // Insert in reverse so clients observe the JDBC-required source order.
        for (int rowIndex = rows.size() - 1; rowIndex >= 0; rowIndex--) {
            final List<Object> row = rows.get(rowIndex);
            if (row.size() != columns.size()) {
                throw new SQLException("Metadata row has " + row.size()
                        + " values but " + columns.size() + " columns were declared.");
            }
            rowSet.moveToInsertRow();
            for (int index = 0; index < row.size(); index++) {
                rowSet.updateObject(index + 1, row.get(index));
            }
            rowSet.insertRow();
            rowSet.moveToCurrentRow();
        }
        rowSet.beforeFirst();
        rowSet.setType(ResultSet.TYPE_FORWARD_ONLY);
        rowSet.setConcurrency(ResultSet.CONCUR_READ_ONLY);
        rowSet.setReadOnly(true);
        return rowSet;
    }

    private ResultSet resultSet(final MetadataResultResponse response) throws SQLException {
        final List<MetadataColumn> columns = new ArrayList<>();
        if (response.getColumns() != null) {
            for (ColumnInfo column : response.getColumns()) {
                columns.add(new MetadataColumn(
                        column.getColumnName(),
                        column.getJdbcType(),
                        column.getTypeName()
                ));
            }
        }
        final List<List<Object>> rows = response.getRows() == null
                ? Collections.emptyList()
                : response.getRows();
        return this.resultSet(columns, rows);
    }

    private String stringArg(final Object[] args, final int index) {
        if (args == null || index >= args.length || args[index] == null) {
            return null;
        }
        return String.valueOf(args[index]);
    }

    private String[] stringArrayArg(final Object[] args, final int index) {
        if (args == null || index >= args.length || args[index] == null) {
            return null;
        }
        return (String[]) args[index];
    }

    private Object defaultValue(final Method method) throws SQLFeatureNotSupportedException {
        final Class<?> returnType = method.getReturnType();
        if (returnType == Boolean.TYPE) {
            return false;
        }
        if (returnType == Integer.TYPE) {
            return 0;
        }
        if (returnType == Long.TYPE) {
            return 0L;
        }
        if (returnType == Short.TYPE) {
            return (short) 0;
        }
        if (returnType == Byte.TYPE) {
            return (byte) 0;
        }
        if (returnType == Float.TYPE) {
            return 0F;
        }
        if (returnType == Double.TYPE) {
            return 0D;
        }
        if (returnType == Void.TYPE) {
            return null;
        }
        if (returnType == String.class) {
            return "";
        }
        if (returnType == ResultSet.class) {
            throw this.unsupported("DatabaseMetaData method is not supported yet: " + method.getName());
        }
        throw this.unsupported("DatabaseMetaData method is not supported yet: " + method.getName());
    }

    private Object unwrap(final Object proxy, final Class<?> iface) throws SQLException {
        if (iface == null) {
            throw new SQLException("Interface must not be null.", "HY009");
        }
        if (iface.isInstance(proxy)) {
            return iface.cast(proxy);
        }
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("DatabaseMetaData does not wrap " + iface.getName());
    }

    private boolean isWrapperFor(
            final Object proxy,
            final Class<?> iface
    ) throws SQLException {
        if (iface == null) {
            throw new SQLException("Interface must not be null.", "HY009");
        }
        return iface.isInstance(proxy) || iface.isInstance(this);
    }

    private void ensureOpen() throws SQLException {
        if (this.connection.isClosed()) {
            throw new SQLException("Wayang JDBC connection is closed.", "08003");
        }
    }

    private String version() {
        final Package driverPackage = WayangDriver.class.getPackage();
        final String version = driverPackage == null ? null : driverPackage.getImplementationVersion();
        return version == null || version.isBlank() ? FALLBACK_VERSION : version;
    }

    private String databaseVersion() {
        final String serverVersion = this.connection.getClient().getServerVersion();
        return serverVersion == null || serverVersion.isBlank() ? this.version() : serverVersion;
    }

    private int databaseVersionPart(final int index, final int fallback) {
        final String[] parts = this.databaseVersion().split("[.-]");
        if (index >= parts.length) {
            return fallback;
        }
        try {
            return Integer.parseInt(parts[index]);
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private String emptyIfNull(final String value) {
        return value == null ? "" : value;
    }

    private SQLFeatureNotSupportedException unsupported(final String message) {
        return new SQLFeatureNotSupportedException(message, "0A000");
    }

    private static MetadataColumn column(final String name, final int type) {
        return new MetadataColumn(name, type);
    }

    private static final class MetadataColumn {

        private final String name;

        private final int type;

        private final String typeName;

        private MetadataColumn(final String name, final int type) {
            this(name, type, jdbcTypeName(type));
        }

        private MetadataColumn(final String name, final int type, final String typeName) {
            this.name = name == null ? "" : name;
            this.type = type;
            this.typeName = typeName == null || typeName.isBlank() ? jdbcTypeName(type) : typeName;
        }
    }

    private static String jdbcTypeName(final int type) {
        switch (type) {
            case Types.BIGINT:
                return "BIGINT";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.INTEGER:
                return "INTEGER";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.VARCHAR:
                return "VARCHAR";
            default:
                return "OTHER";
        }
    }
}
