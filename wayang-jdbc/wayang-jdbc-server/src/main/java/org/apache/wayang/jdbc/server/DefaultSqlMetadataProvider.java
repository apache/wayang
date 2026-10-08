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

import org.apache.wayang.api.sql.context.SqlCatalogMetadata;
import org.apache.wayang.api.sql.context.SqlColumn;
import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.api.sql.context.SqlSchemaMetadata;
import org.apache.wayang.api.sql.context.SqlTableMetadata;
import org.apache.wayang.jdbc.protocol.message.ColumnInfo;
import org.apache.wayang.jdbc.protocol.message.GetColumnsRequest;
import org.apache.wayang.jdbc.protocol.message.GetSchemasRequest;
import org.apache.wayang.jdbc.protocol.message.GetTablesRequest;
import org.apache.wayang.jdbc.protocol.message.MetadataResultResponse;
import org.apache.wayang.jdbc.protocol.message.MetadataType;

import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Metadata provider for the read-only Wayang JDBC gateway.
 *
 * <p>When constructed with a {@link SqlContext}, metadata is read from the
 * same Calcite catalog used for query execution. The no-argument constructor
 * retains the original session-only fallback for custom query executors that
 * do not expose a catalog.</p>
 */
public class DefaultSqlMetadataProvider implements SqlMetadataProvider {

    private static final char SEARCH_ESCAPE = '\\';

    private static final Comparator<String> NULL_SAFE_STRING_COMPARATOR =
            Comparator.nullsFirst(Comparator.naturalOrder());

    private final SqlContext sqlContext;

    public DefaultSqlMetadataProvider() {
        this.sqlContext = null;
    }

    public DefaultSqlMetadataProvider(final SqlContext sqlContext) {
        if (sqlContext == null) {
            throw new IllegalArgumentException("SQL context must not be null.");
        }
        this.sqlContext = sqlContext;
    }

    @Override
    public MetadataResultResponse getSchemas(
            final JdbcServerSession session,
            final GetSchemasRequest request
    ) throws SQLException {
        final String catalog = session.getDatabase();
        final List<List<Object>> rows = new ArrayList<>();
        if (this.matchesCatalog(catalog, request.getCatalog())) {
            for (final SqlSchemaMetadata schema : this.getSchemas(session)) {
                if (this.matchesPattern(schema.getName(), request.getSchemaPattern())) {
                    rows.add(Arrays.asList(schema.getName(), catalog));
                }
            }
        }
        rows.sort(Comparator
                .comparing((List<Object> row) -> (String) row.get(1), NULL_SAFE_STRING_COMPARATOR)
                .thenComparing(row -> (String) row.get(0), NULL_SAFE_STRING_COMPARATOR));

        return new MetadataResultResponse(
                session.getConnectionId(),
                MetadataType.SCHEMAS,
                this.schemasColumns(),
                rows
        );
    }

    @Override
    public MetadataResultResponse getTables(
            final JdbcServerSession session,
            final GetTablesRequest request
    ) throws SQLException {
        final String catalog = session.getDatabase();
        final List<List<Object>> rows = new ArrayList<>();
        if (this.matchesCatalog(catalog, request.getCatalogPattern())) {
            for (final SqlSchemaMetadata schema : this.getSchemas(session)) {
                if (!this.matchesPattern(schema.getName(), request.getSchemaPattern())) {
                    continue;
                }
                for (final SqlTableMetadata table : schema.getTables()) {
                    if (this.matchesPattern(table.getName(), request.getTableNamePattern())
                            && this.matchesTableType(table.getType(), request.getTableTypes())) {
                        rows.add(Arrays.asList(
                                catalog,
                                schema.getName(),
                                table.getName(),
                                table.getType(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null
                        ));
                    }
                }
            }
        }
        rows.sort(Comparator
                .comparing((List<Object> row) -> (String) row.get(3), NULL_SAFE_STRING_COMPARATOR)
                .thenComparing(row -> (String) row.get(0), NULL_SAFE_STRING_COMPARATOR)
                .thenComparing(row -> (String) row.get(1), NULL_SAFE_STRING_COMPARATOR)
                .thenComparing(row -> (String) row.get(2), NULL_SAFE_STRING_COMPARATOR));

        return new MetadataResultResponse(
                session.getConnectionId(),
                MetadataType.TABLES,
                this.tablesColumns(),
                rows
        );
    }

    @Override
    public MetadataResultResponse getColumns(
            final JdbcServerSession session,
            final GetColumnsRequest request
    ) throws SQLException {
        final String catalog = session.getDatabase();
        final List<List<Object>> rows = new ArrayList<>();
        if (this.matchesCatalog(catalog, request.getCatalogPattern())) {
            for (final SqlSchemaMetadata schema : this.getSchemas(session)) {
                if (!this.matchesPattern(schema.getName(), request.getSchemaPattern())) {
                    continue;
                }
                for (final SqlTableMetadata table : schema.getTables()) {
                    if (!this.matchesPattern(table.getName(), request.getTableNamePattern())) {
                        continue;
                    }
                    final List<SqlColumn> columns = table.getColumns();
                    for (int index = 0; index < columns.size(); index++) {
                        final SqlColumn column = columns.get(index);
                        if (this.matchesPattern(column.getName(), request.getColumnNamePattern())) {
                            rows.add(this.columnRow(
                                    catalog,
                                    schema.getName(),
                                    table.getName(),
                                    column,
                                    index + 1
                            ));
                        }
                    }
                }
            }
        }
        rows.sort(Comparator
                .comparing((List<Object> row) -> (String) row.get(0), NULL_SAFE_STRING_COMPARATOR)
                .thenComparing(row -> (String) row.get(1), NULL_SAFE_STRING_COMPARATOR)
                .thenComparing(row -> (String) row.get(2), NULL_SAFE_STRING_COMPARATOR)
                .thenComparingInt(row -> (Integer) row.get(16)));

        return new MetadataResultResponse(
                session.getConnectionId(),
                MetadataType.COLUMNS,
                this.columnsColumns(),
                rows
        );
    }

    private List<SqlSchemaMetadata> getSchemas(final JdbcServerSession session) throws SQLException {
        if (this.sqlContext != null) {
            final SqlCatalogMetadata catalog = this.sqlContext.getCatalogMetadata();
            return catalog.getSchemas();
        }
        final String schema = session.getDatabase();
        if (schema == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(new SqlSchemaMetadata(schema, Collections.emptyList()));
    }

    private List<Object> columnRow(
            final String catalog,
            final String schema,
            final String table,
            final SqlColumn column,
            final int ordinal
    ) {
        final int nullable = column.isNullable()
                ? DatabaseMetaData.columnNullable
                : DatabaseMetaData.columnNoNulls;
        return Arrays.asList(
                catalog,
                schema,
                table,
                column.getName(),
                column.getJdbcType(),
                column.getTypeName(),
                this.nonNegative(column.getPrecision()),
                null,
                this.nonNegative(column.getScale()),
                this.numericRadix(column.getJdbcType()),
                nullable,
                null,
                null,
                null,
                null,
                this.characterOctetLength(column),
                ordinal,
                column.isNullable() ? "YES" : "NO",
                null,
                null,
                null,
                null,
                "NO",
                "NO"
        );
    }

    private Integer nonNegative(final int value) {
        return Math.max(0, value);
    }

    private Integer numericRadix(final int jdbcType) {
        switch (jdbcType) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return 10;
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return 2;
            default:
                return null;
        }
    }

    private Integer characterOctetLength(final SqlColumn column) {
        switch (column.getJdbcType()) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return this.nonNegative(column.getPrecision());
            default:
                return null;
        }
    }

    private List<ColumnInfo> schemasColumns() {
        return Arrays.asList(
                column("TABLE_SCHEM", Types.VARCHAR),
                column("TABLE_CATALOG", Types.VARCHAR)
        );
    }

    private List<ColumnInfo> tablesColumns() {
        return Arrays.asList(
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
        );
    }

    private List<ColumnInfo> columnsColumns() {
        return Arrays.asList(
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
        );
    }

    private boolean matchesTableType(final String value, final List<String> requestedTypes) {
        if (requestedTypes == null) {
            return true;
        }
        if (requestedTypes.isEmpty()) {
            return false;
        }
        for (final String requestedType : requestedTypes) {
            if (requestedType != null && value.equals(requestedType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Catalog arguments in the JDBC metadata API are identifiers, not search
     * patterns. A {@code null} argument accepts any catalog and the empty
     * string selects objects that have no catalog.
     */
    private boolean matchesCatalog(final String value, final String catalog) {
        if (catalog == null) {
            return true;
        }
        if (catalog.isEmpty()) {
            return value == null || value.isEmpty();
        }
        return catalog.equals(value);
    }

    private boolean matchesPattern(final String value, final String pattern) {
        if (pattern == null) {
            return true;
        }
        if (value == null) {
            return pattern.isEmpty();
        }
        return Pattern.compile(
                this.toRegex(pattern),
                Pattern.DOTALL
        ).matcher(value).matches();
    }

    private String toRegex(final String pattern) {
        final StringBuilder regex = new StringBuilder();
        for (int index = 0; index < pattern.length(); index++) {
            final char current = pattern.charAt(index);
            if (current == SEARCH_ESCAPE) {
                if (index + 1 < pattern.length()) {
                    regex.append(Pattern.quote(String.valueOf(pattern.charAt(++index))));
                } else {
                    regex.append(Pattern.quote(String.valueOf(SEARCH_ESCAPE)));
                }
            } else if (current == '%') {
                regex.append(".*");
            } else if (current == '_') {
                regex.append('.');
            } else {
                regex.append(Pattern.quote(String.valueOf(current)));
            }
        }
        return regex.toString();
    }

    private static ColumnInfo column(final String name, final int jdbcType) {
        return new ColumnInfo(
                name,
                name,
                "",
                "",
                jdbcTypeName(jdbcType),
                jdbcType,
                ResultSetMetaData.columnNullable,
                0,
                0
        );
    }

    private static String jdbcTypeName(final int jdbcType) {
        switch (jdbcType) {
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
