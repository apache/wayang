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

package org.apache.wayang.api.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC DatabaseMetaData implementation for Apache Wayang.
 *
 * Describes the Wayang "database" to external tools:
 * - Product name and version
 * - Supported SQL features
 * - Available schemas and tables
 *
 * Tools like DBeaver call this immediately after connecting
 * to understand what they are talking to.
 */
public class WayangDatabaseMetaData implements DatabaseMetaData {

    private final WayangConnection connection;

    public WayangDatabaseMetaData(final WayangConnection connection) {
        this.connection = connection;
    }

    // -------------------------------------------------------------------------
    // Product identity — what is this database?
    // -------------------------------------------------------------------------

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Apache Wayang";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "1.1.2-SNAPSHOT";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "Wayang JDBC Driver";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return WayangDriver.MAJOR_VERSION + "." + WayangDriver.MINOR_VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return WayangDriver.MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return WayangDriver.MINOR_VERSION;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 2;
    }

    // -------------------------------------------------------------------------
    // Connection info
    // -------------------------------------------------------------------------

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public String getURL() throws SQLException {
        return connection.getCatalog();
    }

    @Override
    public String getUserName() throws SQLException {
        return "wayang";
    }

    // -------------------------------------------------------------------------
    // SQL syntax support
    // -------------------------------------------------------------------------

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "ABS,CEIL,FLOOR,ROUND,MOD";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "CONCAT,LOWER,UPPER,TRIM,SUBSTRING,LENGTH";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "DATABASE,USER,IFNULL";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    // -------------------------------------------------------------------------
    // Feature support flags
    // -------------------------------------------------------------------------

    @Override public boolean allProceduresAreCallable() throws SQLException { return false; }
    @Override public boolean allTablesAreSelectable() throws SQLException { return true; }
    @Override public boolean isReadOnly() throws SQLException { return true; }
    @Override public boolean nullsAreSortedHigh() throws SQLException { return false; }
    @Override public boolean nullsAreSortedLow() throws SQLException { return true; }
    @Override public boolean nullsAreSortedAtStart() throws SQLException { return false; }
    @Override public boolean nullsAreSortedAtEnd() throws SQLException { return false; }
    @Override public boolean usesLocalFiles() throws SQLException { return false; }
    @Override public boolean usesLocalFilePerTable() throws SQLException { return false; }
    @Override public boolean supportsMixedCaseIdentifiers() throws SQLException { return false; }
    @Override public boolean storesUpperCaseIdentifiers() throws SQLException { return false; }
    @Override public boolean storesLowerCaseIdentifiers() throws SQLException { return true; }
    @Override public boolean storesMixedCaseIdentifiers() throws SQLException { return false; }
    @Override public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException { return true; }
    @Override public boolean storesUpperCaseQuotedIdentifiers() throws SQLException { return false; }
    @Override public boolean storesLowerCaseQuotedIdentifiers() throws SQLException { return false; }
    @Override public boolean storesMixedCaseQuotedIdentifiers() throws SQLException { return true; }
    @Override public boolean supportsAlterTableWithAddColumn() throws SQLException { return false; }
    @Override public boolean supportsAlterTableWithDropColumn() throws SQLException { return false; }
    @Override public boolean supportsColumnAliasing() throws SQLException { return true; }
    @Override public boolean nullPlusNonNullIsNull() throws SQLException { return true; }
    @Override public boolean supportsConvert() throws SQLException { return false; }
    @Override public boolean supportsConvert(int fromType, int toType) throws SQLException { return false; }
    @Override public boolean supportsTableCorrelationNames() throws SQLException { return true; }
    @Override public boolean supportsDifferentTableCorrelationNames() throws SQLException { return false; }
    @Override public boolean supportsExpressionsInOrderBy() throws SQLException { return true; }
    @Override public boolean supportsOrderByUnrelated() throws SQLException { return false; }
    @Override public boolean supportsGroupBy() throws SQLException { return true; }
    @Override public boolean supportsGroupByUnrelated() throws SQLException { return false; }
    @Override public boolean supportsGroupByBeyondSelect() throws SQLException { return false; }
    @Override public boolean supportsLikeEscapeClause() throws SQLException { return false; }
    @Override public boolean supportsMultipleResultSets() throws SQLException { return false; }
    @Override public boolean supportsMultipleTransactions() throws SQLException { return false; }
    @Override public boolean supportsNonNullableColumns() throws SQLException { return false; }
    @Override public boolean supportsMinimumSQLGrammar() throws SQLException { return true; }
    @Override public boolean supportsCoreSQLGrammar() throws SQLException { return false; }
    @Override public boolean supportsExtendedSQLGrammar() throws SQLException { return false; }
    @Override public boolean supportsANSI92EntryLevelSQL() throws SQLException { return true; }
    @Override public boolean supportsANSI92IntermediateSQL() throws SQLException { return false; }
    @Override public boolean supportsANSI92FullSQL() throws SQLException { return false; }
    @Override public boolean supportsIntegrityEnhancementFacility() throws SQLException { return false; }
    @Override public boolean supportsOuterJoins() throws SQLException { return true; }
    @Override public boolean supportsFullOuterJoins() throws SQLException { return false; }
    @Override public boolean supportsLimitedOuterJoins() throws SQLException { return true; }
    @Override public boolean isCatalogAtStart() throws SQLException { return true; }
    @Override public boolean supportsSchemasInDataManipulation() throws SQLException { return true; }
    @Override public boolean supportsSchemasInProcedureCalls() throws SQLException { return false; }
    @Override public boolean supportsSchemasInTableDefinitions() throws SQLException { return false; }
    @Override public boolean supportsSchemasInIndexDefinitions() throws SQLException { return false; }
    @Override public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException { return false; }
    @Override public boolean supportsCatalogsInDataManipulation() throws SQLException { return false; }
    @Override public boolean supportsCatalogsInProcedureCalls() throws SQLException { return false; }
    @Override public boolean supportsCatalogsInTableDefinitions() throws SQLException { return false; }
    @Override public boolean supportsCatalogsInIndexDefinitions() throws SQLException { return false; }
    @Override public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException { return false; }
    @Override public boolean supportsPositionedDelete() throws SQLException { return false; }
    @Override public boolean supportsPositionedUpdate() throws SQLException { return false; }
    @Override public boolean supportsSelectForUpdate() throws SQLException { return false; }
    @Override public boolean supportsStoredProcedures() throws SQLException { return false; }
    @Override public boolean supportsSubqueriesInComparisons() throws SQLException { return true; }
    @Override public boolean supportsSubqueriesInExists() throws SQLException { return true; }
    @Override public boolean supportsSubqueriesInIns() throws SQLException { return true; }
    @Override public boolean supportsSubqueriesInQuantifieds() throws SQLException { return false; }
    @Override public boolean supportsCorrelatedSubqueries() throws SQLException { return false; }
    @Override public boolean supportsUnion() throws SQLException { return false; }
    @Override public boolean supportsUnionAll() throws SQLException { return false; }
    @Override public boolean supportsOpenCursorsAcrossCommit() throws SQLException { return false; }
    @Override public boolean supportsOpenCursorsAcrossRollback() throws SQLException { return false; }
    @Override public boolean supportsOpenStatementsAcrossCommit() throws SQLException { return false; }
    @Override public boolean supportsOpenStatementsAcrossRollback() throws SQLException { return false; }
    @Override public boolean supportsTransactions() throws SQLException { return false; }
    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException { return false; }
    @Override public boolean supportsDataManipulationTransactionsOnly() throws SQLException { return false; }
    @Override public boolean dataDefinitionCausesTransactionCommit() throws SQLException { return false; }
    @Override public boolean dataDefinitionIgnoredInTransactions() throws SQLException { return false; }
    @Override public boolean supportsBatchUpdates() throws SQLException { return false; }
    @Override public boolean supportsNamedParameters() throws SQLException { return false; }
    @Override public boolean supportsMultipleOpenResults() throws SQLException { return false; }
    @Override public boolean supportsGetGeneratedKeys() throws SQLException { return false; }
    @Override public boolean supportsResultSetType(int type) throws SQLException { return type == ResultSet.TYPE_FORWARD_ONLY; }
    @Override public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException { return concurrency == ResultSet.CONCUR_READ_ONLY; }
    @Override public boolean ownUpdatesAreVisible(int type) throws SQLException { return false; }
    @Override public boolean ownDeletesAreVisible(int type) throws SQLException { return false; }
    @Override public boolean ownInsertsAreVisible(int type) throws SQLException { return false; }
    @Override public boolean othersUpdatesAreVisible(int type) throws SQLException { return false; }
    @Override public boolean othersDeletesAreVisible(int type) throws SQLException { return false; }
    @Override public boolean othersInsertsAreVisible(int type) throws SQLException { return false; }
    @Override public boolean updatesAreDetected(int type) throws SQLException { return false; }
    @Override public boolean deletesAreDetected(int type) throws SQLException { return false; }
    @Override public boolean insertsAreDetected(int type) throws SQLException { return false; }
    @Override public boolean locatorsUpdateCopy() throws SQLException { return false; }
    @Override public boolean supportsStatementPooling() throws SQLException { return false; }
    @Override public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException { return false; }
    @Override public boolean autoCommitFailureClosesAllResultSets() throws SQLException { return false; }
    @Override public boolean generatedKeyAlwaysReturned() throws SQLException { return false; }

    // -------------------------------------------------------------------------
    // Limits
    // -------------------------------------------------------------------------

    @Override public int getMaxBinaryLiteralLength() throws SQLException { return 0; }
    @Override public int getMaxCharLiteralLength() throws SQLException { return 0; }
    @Override public int getMaxColumnNameLength() throws SQLException { return 0; }
    @Override public int getMaxColumnsInGroupBy() throws SQLException { return 0; }
    @Override public int getMaxColumnsInIndex() throws SQLException { return 0; }
    @Override public int getMaxColumnsInOrderBy() throws SQLException { return 0; }
    @Override public int getMaxColumnsInSelect() throws SQLException { return 0; }
    @Override public int getMaxColumnsInTable() throws SQLException { return 0; }
    @Override public int getMaxConnections() throws SQLException { return 0; }
    @Override public int getMaxCursorNameLength() throws SQLException { return 0; }
    @Override public int getMaxIndexLength() throws SQLException { return 0; }
    @Override public int getMaxSchemaNameLength() throws SQLException { return 0; }
    @Override public int getMaxProcedureNameLength() throws SQLException { return 0; }
    @Override public int getMaxCatalogNameLength() throws SQLException { return 0; }
    @Override public int getMaxRowSize() throws SQLException { return 0; }
    @Override public boolean doesMaxRowSizeIncludeBlobs() throws SQLException { return false; }
    @Override public int getMaxStatementLength() throws SQLException { return 0; }
    @Override public int getMaxStatements() throws SQLException { return 0; }
    @Override public int getMaxTableNameLength() throws SQLException { return 0; }
    @Override public int getMaxTablesInSelect() throws SQLException { return 0; }
    @Override public int getMaxUserNameLength() throws SQLException { return 0; }

    // -------------------------------------------------------------------------
    // Transactions
    // -------------------------------------------------------------------------

    @Override
    public boolean supportsTransactionIsolationLevel(final int level) throws SQLException {
        return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    // -------------------------------------------------------------------------
    // Schema / Table / Column metadata ResultSets (return empty sets)
    // -------------------------------------------------------------------------

    @Override
    public ResultSet getSchemas() throws SQLException {
        final java.util.List<org.apache.wayang.basic.data.Record> rows = new ArrayList<>();
        final java.util.List<String> colNames = java.util.Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG");
        rows.add(new org.apache.wayang.basic.data.Record(new Object[]{"wayang", "wayang"}));
        final WayangResultSet rs = new WayangResultSet(rows, "getSchemas");
        rs.overrideColumnNames(colNames);
        return rs;
    }

    @Override
    public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException {
        return getSchemas();
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getTables(final String catalog, final String schemaPattern,
            final String tableNamePattern, final String[] types) throws SQLException {
        final java.util.List<org.apache.wayang.basic.data.Record> rows = new ArrayList<>();
        final java.util.List<String> colNames = java.util.Arrays.asList(
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE",
                "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
                "SELF_REFERENCING_COL_NAME", "REF_GENERATION");
        rows.add(new org.apache.wayang.basic.data.Record(new Object[]{
                "wayang", "wayang", "wayang_table", "TABLE",
                "Apache Wayang virtual table", null, null, null, null, null}));
        final WayangResultSet rs = new WayangResultSet(rows, "getTables");
        rs.overrideColumnNames(colNames);
        return rs;
    }

    @Override
    public ResultSet getColumns(final String catalog, final String schemaPattern,
            final String tableNamePattern, final String columnNamePattern) throws SQLException {
        final java.util.List<org.apache.wayang.basic.data.Record> rows = new ArrayList<>();
        final java.util.List<String> colNames = java.util.Arrays.asList(
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
                "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
                "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
                "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
                "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
                "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN");
        final WayangResultSet rs = new WayangResultSet(rows, "getColumns");
        rs.overrideColumnNames(colNames);
        return rs;
    }

    @Override
    public ResultSet getPrimaryKeys(final String catalog, final String schema,
            final String table) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getProcedures(final String catalog, final String schemaPattern,
            final String procedureNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getProcedureColumns(final String catalog, final String schemaPattern,
            final String procedureNamePattern, final String columnNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getColumnPrivileges(final String catalog, final String schema,
            final String table, final String columnNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getTablePrivileges(final String catalog, final String schemaPattern,
            final String tableNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getBestRowIdentifier(final String catalog, final String schema,
            final String table, final int scope, final boolean nullable) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getVersionColumns(final String catalog, final String schema,
            final String table) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getImportedKeys(final String catalog, final String schema,
            final String table) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getExportedKeys(final String catalog, final String schema,
            final String table) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getCrossReference(final String parentCatalog, final String parentSchema,
            final String parentTable, final String foreignCatalog, final String foreignSchema,
            final String foreignTable) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getIndexInfo(final String catalog, final String schema, final String table,
            final boolean unique, final boolean approximate) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getUDTs(final String catalog, final String schemaPattern,
            final String typeNamePattern, final int[] types) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getSuperTypes(final String catalog, final String schemaPattern,
            final String typeNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getSuperTables(final String catalog, final String schemaPattern,
            final String tableNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getAttributes(final String catalog, final String schemaPattern,
            final String typeNamePattern, final String attributeNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getFunctions(final String catalog, final String schemaPattern,
            final String functionNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getFunctionColumns(final String catalog, final String schemaPattern,
            final String functionNamePattern, final String columnNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getPseudoColumns(final String catalog, final String schemaPattern,
            final String tableNamePattern, final String columnNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean supportsResultSetHoldability(final int holdability) throws SQLException {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    // -------------------------------------------------------------------------
    // Unwrap
    // -------------------------------------------------------------------------

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    /** Returns an empty ResultSet for metadata queries that have no data yet */
    private ResultSet emptyResultSet() throws SQLException {
        return new WayangResultSet(new ArrayList<>(), "");
    }
}