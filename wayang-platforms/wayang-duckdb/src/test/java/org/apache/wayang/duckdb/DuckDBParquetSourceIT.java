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

package org.apache.wayang.duckdb;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.duckdb.operators.DuckDBParquetSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for {@link DuckDBParquetSource}.
 */
class DuckDBParquetSourceIT {

    private static final String SCHEMA = "wayang_parquet_it";
    private static final String SINK_TABLE = SCHEMA + ".orders_parquet_copy";
    private static final String MAPPED_SINK_TABLE = SCHEMA + ".orders_mapped_copy";
    private static final String GCS_SINK_TABLE = SCHEMA + ".orders_gcs_copy";
    private static final String SOURCE_VIEW = SCHEMA + ".orders_parquet";
    private static final String[] COLUMNS = {"order_id", "region", "amount"};
    private static final String DEFAULT_GCS_URI =
            "gs://anaconda-public-data/nyc-taxi/nyc.parquet/part.0.parquet";

    private static Path databaseFile;
    private static Path parquetFile;
    private static String jdbcUrl;

    @BeforeAll
    static void setUp() throws Exception {
        databaseFile = Files.createTempFile("wayang-duckdb-parquet-", ".duckdb");
        parquetFile = Files.createTempFile("wayang-duckdb-orders-", ".parquet");
        Files.deleteIfExists(databaseFile);
        Files.deleteIfExists(parquetFile);
        jdbcUrl = "jdbc:duckdb:" + databaseFile.toAbsolutePath();

        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
            statement.execute("DROP TABLE IF EXISTS " + SINK_TABLE);
            statement.execute("DROP TABLE IF EXISTS " + MAPPED_SINK_TABLE);
            statement.execute("DROP TABLE IF EXISTS " + GCS_SINK_TABLE);
            statement.execute("DROP VIEW IF EXISTS " + SOURCE_VIEW);
            statement.execute("COPY ("
                    + "SELECT * FROM (VALUES "
                    + "(CAST(1 AS BIGINT), 'AMER', CAST(10.0 AS DOUBLE)), "
                    + "(CAST(2 AS BIGINT), 'EMEA', CAST(20.0 AS DOUBLE)), "
                    + "(CAST(3 AS BIGINT), 'APAC', CAST(30.0 AS DOUBLE))"
                    + ") AS t(order_id, region, amount)"
                    + ") TO '" + parquetUri() + "' (FORMAT PARQUET)");
            statement.execute("CREATE VIEW " + SOURCE_VIEW + " AS SELECT * FROM read_parquet('"
                    + parquetUri() + "')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (databaseFile != null) {
            Files.deleteIfExists(databaseFile);
            Files.deleteIfExists(databaseFile.resolveSibling(databaseFile.getFileName() + ".wal"));
        }
        if (parquetFile != null) {
            Files.deleteIfExists(parquetFile);
        }
    }

    @Test
    void readsLocalParquetFileViaAutoCreatedDuckDbView() throws Exception {
        DuckDBParquetSource source = new DuckDBParquetSource(parquetUri(), null, COLUMNS);
        TableSink<Record> sink = new TableSink<>(new Properties(), "overwrite", SINK_TABLE, COLUMNS);
        source.connectTo(0, sink, 0);

        wayangContext(true, "").execute(new WayangPlan(sink));

        assertEquals(3, queryLong("SELECT count(*) FROM " + SINK_TABLE));
        assertEquals(60.0, queryDouble("SELECT sum(amount) FROM " + SINK_TABLE), 0.01);
    }

    @Test
    void mapsParquetUriToExistingDuckDbRelation() throws Exception {
        DuckDBParquetSource source = new DuckDBParquetSource(parquetUri(), null, COLUMNS);
        TableSink<Record> sink = new TableSink<>(new Properties(), "overwrite", MAPPED_SINK_TABLE, COLUMNS);
        source.connectTo(0, sink, 0);

        Configuration configuration = baseConfiguration();
        configuration.setProperty("wayang.duckdb.parquetsource.mappings", parquetUri() + "=" + SOURCE_VIEW);
        new WayangContext(configuration).withPlugin(DuckDB.plugin()).execute(new WayangPlan(sink));

        assertEquals(3, queryLong("SELECT count(*) FROM " + MAPPED_SINK_TABLE));
        assertEquals(60.0, queryDouble("SELECT sum(amount) FROM " + MAPPED_SINK_TABLE), 0.01);
    }

    @Test
    void readsPublicGcsParquetFileViaDuckDbHttpfs() throws Exception {
        String gcsUri = System.getProperty("duckdb.gcs.parquet.uri",
                System.getenv().getOrDefault("DUCKDB_GCS_PARQUET_URI", DEFAULT_GCS_URI));
        Assumptions.assumeTrue(isGcsParquetReachable(gcsUri), "DuckDB httpfs cannot reach " + gcsUri);

        DuckDBParquetSource source = new DuckDBParquetSource(gcsUri, null);
        TableSink<Record> sink = new TableSink<>(new Properties(), "overwrite", GCS_SINK_TABLE);
        source.connectTo(0, sink, 0);

        wayangContext(true, "INSTALL httpfs; LOAD httpfs").execute(new WayangPlan(sink));

        assertEquals(
                queryLong("SELECT count(*) FROM read_parquet('" + gcsUri + "')"),
                queryLong("SELECT count(*) FROM " + GCS_SINK_TABLE));
    }

    private WayangContext wayangContext(boolean autoCreate, String prepareSql) {
        Configuration configuration = baseConfiguration();
        configuration.setProperty("wayang.duckdb.parquetsource.auto-create", Boolean.toString(autoCreate));
        if (!prepareSql.isEmpty()) {
            configuration.setProperty("wayang.duckdb.parquetsource.prepare-sql", prepareSql);
        }
        return new WayangContext(configuration).withPlugin(DuckDB.plugin());
    }

    private static Configuration baseConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.duckdb.jdbc.url", jdbcUrl);
        configuration.setProperty("wayang.duckdb.jdbc.user", "");
        configuration.setProperty("wayang.duckdb.jdbc.password", "");
        return configuration;
    }

    private static boolean isGcsParquetReachable(String gcsUri) {
        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            statement.execute("INSTALL httpfs");
            statement.execute("LOAD httpfs");
            return queryLong(statement, "SELECT count(*) FROM read_parquet('" + gcsUri + "')") > 0;
        } catch (Exception e) {
            System.err.println("[DuckDBParquetSourceIT] GCS Parquet unavailable: " + e.getMessage());
            return false;
        }
    }

    private static long queryLong(String sql) throws Exception {
        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            return queryLong(statement, sql);
        }
    }

    private static long queryLong(Statement statement, String sql) throws Exception {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
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
        return DriverManager.getConnection(jdbcUrl);
    }

    private static String parquetUri() {
        return parquetFile.toAbsolutePath().toString().replace('\\', '/');
    }
}
