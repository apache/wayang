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

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.wayang.api.sql.calcite.utils.ModelParser;
import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.core.api.Configuration;
import org.json.simple.parser.ParseException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class WayangJdbcIntegrationTest {

    private SqlContext sqlContext;

    @BeforeEach
    public void setUp() throws Exception {
        sqlContext = createSqlContext("/data/exampleInt.csv");
    }

    private WayangConnection createTestConnection() throws SQLException {
        return new WayangConnection("jdbc:wayang:test", sqlContext, null);
    }

    @Test
    public void testConnectionNotClosed() throws Exception {
        final WayangConnection conn = createTestConnection();
        assertFalse(conn.isClosed());
        assertNotNull(conn.getMetaData());
        assertEquals("wayang", conn.getCatalog());
        conn.close();
    }

    @Test
    public void testConnectionClose() throws Exception {
        final WayangConnection conn = createTestConnection();
        assertFalse(conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
    }

    @Test
    public void testCreateStatement() throws Exception {
        final WayangConnection conn = createTestConnection();
        final Statement stmt = conn.createStatement();
        assertNotNull(stmt);
        assertFalse(stmt.isClosed());
        stmt.close();
        conn.close();
    }

    @Test
    public void testStatementExecuteQueryReturnsResultSet() throws Exception {
        final WayangConnection conn = createTestConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM fs.exampleInt");
        assertNotNull(rs);
        rs.close();
        stmt.close();
        conn.close();
    }

    @Test
    public void testResultSetHasRows() throws Exception {
        final WayangConnection conn = createTestConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM fs.exampleInt");
        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
        }
        assertTrue(rowCount > 0, "ResultSet should have at least one row");
        rs.close();
        stmt.close();
        conn.close();
    }

    @Test
    public void testResultSetMetaData() throws Exception {
        final WayangConnection conn = createTestConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM fs.exampleInt");
        final ResultSetMetaData meta = rs.getMetaData();
        assertNotNull(meta);
        assertTrue(meta.getColumnCount() > 0);
        rs.close();
        stmt.close();
        conn.close();
    }

    @Test
    public void testResultSetColumnNames() throws Exception {
        final WayangConnection conn = createTestConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM fs.exampleInt");
        final ResultSetMetaData meta = rs.getMetaData();
        assertTrue(meta.getColumnCount() >= 1);
        rs.close();
        stmt.close();
        conn.close();
    }

    @Test
    public void testPreparedStatementBasic() throws Exception {
        final WayangConnection conn = createTestConnection();
        final PreparedStatement ps = conn.prepareStatement("SELECT * FROM fs.exampleInt");
        assertNotNull(ps);
        final ResultSet rs = ps.executeQuery();
        assertNotNull(rs);
        rs.close();
        ps.close();
        conn.close();
    }

    @Test
    public void testDatabaseMetaData() throws Exception {
        final WayangConnection conn = createTestConnection();
        final DatabaseMetaData meta = conn.getMetaData();
        assertNotNull(meta);
        assertEquals("Apache Wayang", meta.getDatabaseProductName());
        assertEquals("Wayang JDBC Driver", meta.getDriverName());
        assertEquals(4, meta.getJDBCMajorVersion());
        conn.close();
    }

    @Test
    public void testDatabaseMetaDataSupportsSelect() throws Exception {
        final WayangConnection conn = createTestConnection();
        final DatabaseMetaData meta = conn.getMetaData();
        assertTrue(meta.supportsANSI92EntryLevelSQL());
        assertTrue(meta.supportsGroupBy());
        assertTrue(meta.supportsOuterJoins());
        conn.close();
    }

    @Test
    public void testDriverAcceptsUrl() throws Exception {
        final WayangDriver driver = new WayangDriver();
        assertTrue(driver.acceptsURL("jdbc:wayang:/path/to/config"));
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost/db"));
    }

    @Test
    public void testStatementOnClosedConnectionThrows() throws Exception {
        final WayangConnection conn = createTestConnection();
        conn.close();
        assertThrows(SQLException.class, () -> conn.createStatement());
    }

    @Test
    public void testClosedStatementThrows() throws Exception {
        final WayangConnection conn = createTestConnection();
        final Statement stmt = conn.createStatement();
        stmt.close();
        assertThrows(SQLException.class,
                () -> stmt.executeQuery("SELECT * FROM fs.exampleInt"));
        conn.close();
    }

    private SqlContext createSqlContext(final String tableResourceName)
            throws IOException, ParseException, SQLException {
        final String calciteModel = "{\r\n"
                + "    \"calcite\": {\r\n"
                + "      \"version\": \"1.0\",\r\n"
                + "      \"defaultSchema\": \"wayang\",\r\n"
                + "      \"schemas\": [\r\n"
                + "        {\r\n"
                + "          \"name\": \"fs\",\r\n"
                + "          \"type\": \"custom\",\r\n"
                + "          \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\r\n"
                + "          \"operand\": {\r\n"
                + "            \"directory\": \"" + "/"
                + this.getClass().getResource("/data").getPath() + "\"\r\n"
                + "          }\r\n"
                + "        }\r\n"
                + "      ]\r\n"
                + "    }\r\n"
                + "  }";
        final JsonNode calciteModelJSON = new ObjectMapper().readTree(calciteModel);
        final Configuration configuration = new ModelParser(
                new Configuration(), calciteModelJSON).setProperties();
        assertNotNull(configuration);
        final String dataPath = this.getClass().getResource(tableResourceName).getPath();
        assertNotNull(dataPath);
        configuration.setProperty("wayang.fs.table.url", dataPath);
        configuration.setProperty("wayang.ml.executions.file", "mle.txt");
        configuration.setProperty("wayang.ml.optimizations.file", "mlo.txt");
        configuration.setProperty("wayang.ml.experience.enabled", "false");
        return new SqlContext(configuration);
    }
}
