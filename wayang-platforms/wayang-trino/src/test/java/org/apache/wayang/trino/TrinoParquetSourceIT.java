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

package org.apache.wayang.trino;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.trino.operators.TrinoParquetSource;
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
 * Minimal integration test for {@link TrinoParquetSource}.
 */
class TrinoParquetSourceIT {

    private static final String HOST = System.getenv().getOrDefault("TRINO_HOST", "localhost");
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("TRINO_PORT", "8080"));
    private static final String USER = System.getenv().getOrDefault("TRINO_USER", "admin");
    private static final String JDBC_URL = String.format("jdbc:trino://%s:%d", HOST, PORT);

    private static final String SCHEMA = "iceberg.wayang_parquet_it";
    private static final String SOURCE = SCHEMA + ".orders_parquet";
    private static final String SINK = SCHEMA + ".orders_parquet_copy";
    private static final String[] COLUMNS = {"order_id", "region", "amount"};

    private static boolean available = false;

    @BeforeAll
    static void setUp() {
        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
            statement.execute("DROP TABLE IF EXISTS " + SINK);
            statement.execute("DROP TABLE IF EXISTS " + SOURCE);
            statement.execute("CREATE TABLE " + SOURCE + " WITH (format = 'PARQUET') AS "
                    + "SELECT * FROM (VALUES "
                    + "(CAST(1 AS BIGINT), 'AMER', CAST(10.0 AS DOUBLE)), "
                    + "(CAST(2 AS BIGINT), 'EMEA', CAST(20.0 AS DOUBLE)), "
                    + "(CAST(3 AS BIGINT), 'APAC', CAST(30.0 AS DOUBLE))"
                    + ") AS t(order_id, region, amount)");
            available = true;
        } catch (Exception e) {
            System.err.println("[TrinoParquetSourceIT] Trino Parquet setup unavailable: " + e.getMessage());
        }
    }

    @AfterAll
    static void tearDown() {
        if (!available) return;
        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + SINK);
            statement.execute("DROP TABLE IF EXISTS " + SOURCE);
        } catch (Exception e) {
            System.err.println("[TrinoParquetSourceIT] cleanup failed: " + e.getMessage());
        }
    }

    @Test
    void readsParquetBackedRelationIntoTrinoSink() throws Exception {
        Assumptions.assumeTrue(available, "Trino Parquet setup unavailable");

        TrinoParquetSource source = new TrinoParquetSource(SOURCE, null, COLUMNS);
        TableSink<Record> sink = new TableSink<>(new Properties(), "overwrite", SINK, COLUMNS);
        source.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(3, queryLong("SELECT count(*) FROM " + SINK));
        assertEquals(60.0, queryDouble("SELECT sum(amount) FROM " + SINK), 0.01);
    }

    private WayangContext wayangContext() {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.trino.jdbc.url", JDBC_URL);
        configuration.setProperty("wayang.trino.jdbc.user", USER);
        configuration.setProperty("wayang.trino.jdbc.password", "");
        return new WayangContext(configuration).withPlugin(Trino.plugin());
    }

    private static long queryLong(String sql) throws Exception {
        try (Connection connection = jdbc();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private static double queryDouble(String sql) throws Exception {
        try (Connection connection = jdbc();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getDouble(1);
        }
    }

    private static Connection jdbc() throws Exception {
        return DriverManager.getConnection(JDBC_URL, USER, "");
    }
}
