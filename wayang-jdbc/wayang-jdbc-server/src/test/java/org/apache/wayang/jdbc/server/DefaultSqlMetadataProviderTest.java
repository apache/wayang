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
import org.apache.wayang.jdbc.protocol.message.GetColumnsRequest;
import org.apache.wayang.jdbc.protocol.message.GetSchemasRequest;
import org.apache.wayang.jdbc.protocol.message.GetTablesRequest;
import org.apache.wayang.jdbc.protocol.message.MetadataResultResponse;
import org.apache.wayang.jdbc.protocol.message.MetadataType;

import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DefaultSqlMetadataProviderTest {

    @Test
    void getSchemasAppliesExactCatalogAndCaseSensitivePattern() throws Exception {
        final DefaultSqlMetadataProvider provider = this.provider();
        final JdbcServerSession session = session();

        final MetadataResultResponse response = provider.getSchemas(
                session,
                new GetSchemasRequest("connection-1", "analytics", "Pub%")
        );
        assertEquals(MetadataType.SCHEMAS, response.getMetadataType());
        assertEquals(Collections.singletonList(Arrays.asList("Public", "analytics")), response.getRows());

        final MetadataResultResponse lowerCasePatternResponse = provider.getSchemas(
                session,
                new GetSchemasRequest("connection-1", "analytics", "pub%")
        );
        assertEquals(Collections.emptyList(), lowerCasePatternResponse.getRows());

        final MetadataResultResponse catalogPatternResponse = provider.getSchemas(
                session,
                new GetSchemasRequest("connection-1", "ana%", "Pub%")
        );
        assertEquals(Collections.emptyList(), catalogPatternResponse.getRows());
    }

    @Test
    void getTablesAppliesPatternTypeAndSortOrder() throws Exception {
        final DefaultSqlMetadataProvider provider = this.provider();

        final MetadataResultResponse response = provider.getTables(
                session(),
                new GetTablesRequest("connection-1", "analytics", "%", "%PEOPLE", List.of("TABLE"))
        );

        assertEquals(MetadataType.TABLES, response.getMetadataType());
        assertEquals(1, response.getRows().size());
        assertEquals("Public", response.getRows().get(0).get(1));
        assertEquals("PEOPLE", response.getRows().get(0).get(2));
        assertEquals("TABLE", response.getRows().get(0).get(3));

        final MetadataResultResponse emptyTypesResponse = provider.getTables(
                session(),
                new GetTablesRequest("connection-1", "analytics", "%", "%PEOPLE", Collections.emptyList())
        );
        assertEquals(Collections.emptyList(), emptyTypesResponse.getRows());
    }

    @Test
    void getColumnsAppliesEscapedPatternAndColumnAttributes() throws Exception {
        final DefaultSqlMetadataProvider provider = this.provider();

        final MetadataResultResponse response = provider.getColumns(
                session(),
                new GetColumnsRequest("connection-1", "analytics", "Public", "PEOPLE", "NA\\_ME")
        );

        assertEquals(MetadataType.COLUMNS, response.getMetadataType());
        assertEquals(1, response.getRows().size());

        final List<Object> row = response.getRows().get(0);
        assertEquals("analytics", row.get(0));
        assertEquals("Public", row.get(1));
        assertEquals("PEOPLE", row.get(2));
        assertEquals("NA_ME", row.get(3));
        assertEquals(Types.VARCHAR, row.get(4));
        assertEquals("VARCHAR", row.get(5));
        assertEquals(64, row.get(6));
        assertEquals(DatabaseMetaData.columnNullable, row.get(10));
        assertEquals(2, row.get(16));
        assertEquals("YES", row.get(17));
        assertEquals("NO", row.get(22));
        assertEquals("NO", row.get(23));
    }

    @Test
    void getColumnsOrdersByCatalogSchemaTableAndOrdinal() throws Exception {
        final DefaultSqlMetadataProvider provider = this.provider();

        final MetadataResultResponse response = provider.getColumns(
                session(),
                new GetColumnsRequest("connection-1", "analytics", "%", "%", "%")
        );

        assertEquals(Arrays.asList("ID", "NA_ME", "VIEW_ID"), Arrays.asList(
                response.getRows().get(0).get(3),
                response.getRows().get(1).get(3),
                response.getRows().get(2).get(3)
        ));
    }

    private DefaultSqlMetadataProvider provider() throws Exception {
        final SqlContext sqlContext = mock(SqlContext.class);
        when(sqlContext.getCatalogMetadata()).thenReturn(new SqlCatalogMetadata(Collections.singletonList(
                new SqlSchemaMetadata("Public", Arrays.asList(
                        new SqlTableMetadata("PEOPLE", "TABLE", Arrays.asList(
                                new SqlColumn("ID", "ID", "INTEGER", Types.INTEGER, 10, 0, false),
                                new SqlColumn("NA_ME", "NA_ME", "VARCHAR", Types.VARCHAR, 64, 0, true)
                        )),
                        new SqlTableMetadata("V_PEOPLE", "VIEW", Collections.singletonList(
                                new SqlColumn("VIEW_ID", "VIEW_ID", "INTEGER", Types.INTEGER, 10, 0, false)
                        ))
                ))
        )));
        return new DefaultSqlMetadataProvider(sqlContext);
    }

    private static JdbcServerSession session() {
        return new JdbcServerSession(
                "connection-1",
                "client-1",
                "user",
                "analytics",
                Collections.emptyMap()
        );
    }
}
