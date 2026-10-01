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

package org.apache.wayang.presto;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.presto.operators.PrestoParquetSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Minimal integration test for {@link PrestoParquetSource}.
 *
 * <p>Set {@code -Dpresto.parquet.source=catalog.schema.table} or
 * {@code PRESTO_PARQUET_SOURCE} to point at an existing Parquet-backed table.
 */
class PrestoParquetSourceIT {

    private static final String HOST = System.getenv().getOrDefault("PRESTO_HOST", "localhost");
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("PRESTO_PORT", "8081"));
    private static final String USER = System.getenv().getOrDefault("PRESTO_USER", "test");
    private static final String JDBC_URL = cfg("presto.jdbc.url", "PRESTO_JDBC_URL",
            String.format("jdbc:presto://%s:%d/hive", HOST, PORT));
    private static final String SOURCE = cfg("presto.parquet.source", "PRESTO_PARQUET_SOURCE", "");
    private static final String SINK = cfg("presto.parquet.sink", "PRESTO_PARQUET_SINK",
            defaultSinkName(SOURCE, "wayang_parquet_source_copy"));

    private static boolean available = false;
    private static long sourceCount = -1;

    @BeforeAll
    static void setUp() {
        if (SOURCE.isEmpty()) {
            return;
        }
        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            sourceCount = queryLong(statement, "SELECT count(*) FROM " + SOURCE);
            statement.execute("DROP TABLE IF EXISTS " + SINK);
            available = true;
        } catch (Exception e) {
            System.err.println("[PrestoParquetSourceIT] Presto Parquet source unavailable: " + e.getMessage());
        }
    }

    @AfterAll
    static void tearDown() {
        if (!available) return;
        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + SINK);
        } catch (Exception e) {
            System.err.println("[PrestoParquetSourceIT] cleanup failed: " + e.getMessage());
        }
    }

    @Test
    void readsConfiguredParquetBackedRelationIntoPrestoSink() throws Exception {
        Assumptions.assumeFalse(SOURCE.isEmpty(), "No Presto Parquet source configured");
        Assumptions.assumeTrue(available, "Presto Parquet source unavailable");

        PrestoParquetSource source = new PrestoParquetSource(SOURCE, null);
        TableSink<Record> sink = new TableSink<>(new Properties(), "overwrite", SINK);
        source.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(sourceCount, queryLong("SELECT count(*) FROM " + SINK));
    }

    private WayangContext wayangContext() {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.presto.jdbc.url", JDBC_URL);
        configuration.setProperty("wayang.presto.jdbc.user", USER);
        return new WayangContext(configuration).withPlugin(Presto.plugin());
    }

    private static String cfg(String sysProp, String envVar, String dflt) {
        String value = System.getProperty(sysProp);
        if (value == null || value.isEmpty()) value = System.getenv(envVar);
        return value == null || value.isEmpty() ? dflt : value;
    }

    private static String defaultSinkName(String source, String tableName) {
        int lastDot = source.lastIndexOf('.');
        if (lastDot < 0) {
            return tableName;
        }
        return source.substring(0, lastDot + 1) + tableName;
    }

    private static long queryLong(String sql) throws Exception {
        try (Connection connection = jdbc();
                Statement statement = connection.createStatement()) {
            return queryLong(statement, sql);
        }
    }

    private static long queryLong(Statement statement, String sql) throws Exception {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private static Connection jdbc() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        return DriverManager.getConnection(JDBC_URL, properties);
    }
}
