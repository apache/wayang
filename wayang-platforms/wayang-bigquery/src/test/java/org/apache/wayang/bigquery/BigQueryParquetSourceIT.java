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

package org.apache.wayang.bigquery;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.bigquery.operators.BigQueryParquetSource;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
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
 * Minimal integration test for {@link BigQueryParquetSource}.
 *
 * <p>Set {@code -Dbigquery.parquet.source=`project.dataset.table`} or
 * {@code BIGQUERY_PARQUET_SOURCE} for an existing external table. Alternatively,
 * set {@code -Dbigquery.parquet.uri=gs://bucket/path/*.parquet} to let the test
 * create a temporary BigQuery external table.
 */
class BigQueryParquetSourceIT {

    private static final String PROJECT_ID = cfg("bigquery.project", "BIGQUERY_PROJECT", "your-project");
    private static final String SA_EMAIL = cfg("bigquery.saEmail", "BIGQUERY_SA_EMAIL",
            "wayang-bq@" + PROJECT_ID + ".iam.gserviceaccount.com");
    private static final String KEY_PATH = cfg("bigquery.keyPath", "BIGQUERY_KEY_PATH",
            System.getProperty("user.home") + "/wayang-bq-key.json");
    private static final String LOCATION = cfg("bigquery.location", "BIGQUERY_LOCATION", "US");
    private static final String DATASET = cfg("bigquery.dataset", "BIGQUERY_DATASET", "wayang_it");
    private static final String PARQUET_URI = cfg("bigquery.parquet.uri", "BIGQUERY_PARQUET_URI", "");
    private static final String CONFIGURED_SOURCE = cfg("bigquery.parquet.source", "BIGQUERY_PARQUET_SOURCE", "");
    private static final String SOURCE = CONFIGURED_SOURCE.isEmpty() ? tableName("parquet_source") : CONFIGURED_SOURCE;
    private static final String SINK = cfg("bigquery.parquet.sink", "BIGQUERY_PARQUET_SINK",
            tableName("parquet_source_copy"));

    private static final String JDBC_URL = String.format(
            "jdbc:bigquery://https://www.googleapis.com/bigquery/v2;"
                    + "ProjectId=%s;OAuthType=0;OAuthServiceAcctEmail=%s;OAuthPvtKeyPath=%s;Location=%s",
            PROJECT_ID, SA_EMAIL, KEY_PATH, LOCATION);

    private static boolean available = false;
    private static long sourceCount = -1;

    @BeforeAll
    static void setUp() {
        if (CONFIGURED_SOURCE.isEmpty() && PARQUET_URI.isEmpty()) {
            return;
        }
        try {
            Class.forName("com.google.cloud.bigquery.jdbc.BigQueryDriver");
            try (Connection connection = DriverManager.getConnection(JDBC_URL);
                    Statement statement = connection.createStatement()) {
                statement.execute("CREATE SCHEMA IF NOT EXISTS `" + PROJECT_ID + "." + DATASET + "` "
                        + "OPTIONS(location='" + LOCATION + "')");
                statement.execute("DROP TABLE IF EXISTS " + SINK);
                if (!PARQUET_URI.isEmpty()) {
                    statement.execute("DROP TABLE IF EXISTS " + SOURCE);
                    statement.execute("CREATE EXTERNAL TABLE " + SOURCE
                            + " OPTIONS (format = 'PARQUET', uris = ['" + PARQUET_URI + "'])");
                }
                sourceCount = queryLong(statement, "SELECT count(*) FROM " + SOURCE);
                available = true;
            }
        } catch (Exception e) {
            System.err.println("[BigQueryParquetSourceIT] BigQuery Parquet source unavailable: " + e.getMessage());
        }
    }

    @AfterAll
    static void tearDown() {
        if (!available) return;
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + SINK);
            if (!PARQUET_URI.isEmpty()) {
                statement.execute("DROP TABLE IF EXISTS " + SOURCE);
            }
        } catch (Exception e) {
            System.err.println("[BigQueryParquetSourceIT] cleanup failed: " + e.getMessage());
        }
    }

    @Test
    void readsConfiguredParquetExternalTableIntoBigQuerySink() throws Exception {
        Assumptions.assumeTrue(!CONFIGURED_SOURCE.isEmpty() || !PARQUET_URI.isEmpty(),
                "No BigQuery Parquet source or URI configured");
        Assumptions.assumeTrue(available, "BigQuery Parquet source unavailable");

        BigQueryParquetSource source = new BigQueryParquetSource(SOURCE, null);
        TableSink<Record> sink = new TableSink<>(new Properties(), "overwrite", SINK);
        source.connectTo(0, sink, 0);

        wayangContext().execute("BQ-ParquetSource", new WayangPlan(sink));

        assertEquals(sourceCount, queryLong("SELECT count(*) FROM " + SINK));
    }

    private WayangContext wayangContext() {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.bigquery.jdbc.url", JDBC_URL);
        return new WayangContext(configuration).withPlugin(BigQuery.plugin());
    }

    private static String cfg(String sysProp, String envVar, String dflt) {
        String value = System.getProperty(sysProp);
        if (value == null || value.isEmpty()) value = System.getenv(envVar);
        return value == null || value.isEmpty() ? dflt : value;
    }

    private static String tableName(String table) {
        return "`" + PROJECT_ID + "." + DATASET + "." + table + "`";
    }

    private static long queryLong(String sql) throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
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
}
