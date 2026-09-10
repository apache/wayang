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

package org.apache.wayang.applications;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.duckdb.DuckDB;
import org.apache.wayang.duckdb.operators.DuckDBTableSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Configurable DuckDB filter and projection example.
 * See {@code wayang-applications/duckdb.md} for usage and fixture initialization.
 */
public class DuckDBDemo {

    private final Configuration configuration;
    private final String jdbcUrl;
    private final String orders;
    private final String filterResult;
    private final String projectionResult;

    private DuckDBDemo(Configuration configuration) {
        this.configuration = configuration;
        this.jdbcUrl = configuration.getStringProperty("wayang.duckdb.jdbc.url");
        if ("jdbc:duckdb:".equals(jdbcUrl)) {
            throw new IllegalArgumentException("This example requires a database file shared by its JDBC connections.");
        }
        this.orders = configuration.getStringProperty("wayang.duckdb.demo.orders", "wayang_demo.orders");
        this.filterResult = configuration.getStringProperty(
                "wayang.duckdb.demo.filter-result", "wayang_demo.filter_result"
        );
        this.projectionResult = configuration.getStringProperty(
                "wayang.duckdb.demo.projection-result", "wayang_demo.projection_result"
        );
        if (orders.equalsIgnoreCase(filterResult) || orders.equalsIgnoreCase(projectionResult)
                || filterResult.equalsIgnoreCase(projectionResult)) {
            throw new IllegalArgumentException("Input and output tables must have distinct names.");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2 || (args.length == 2 && !"--init".equals(args[1]))) {
            throw new IllegalArgumentException("Usage: DuckDBDemo <configuration URL> [--init]");
        }
        DuckDBDemo demo = new DuckDBDemo(new Configuration(args[0]));
        if (args.length == 2) {
            demo.createFixture();
        }
        demo.runFilterPushdown();
        demo.runProjectionPushdown();
    }

    private void runFilterPushdown() throws Exception {
        System.out.println();
        System.out.println("DuckDB demo: Filter pushdown");
        System.out.println("SQL shape: SELECT * FROM " + orders + " WHERE region = 'AMER'");

        DuckDBTableSource source = new DuckDBTableSource(
                orders, "order_id", "customer_id", "region", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record record) -> "AMER".equals(record.getField(2)), Record.class)
                        .withSqlImplementation("region = 'AMER'"));
        TableSink<Record> sink = new TableSink<>(
                new Properties(), "overwrite", filterResult,
                "order_id", "customer_id", "region", "amount");

        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);
        wayangContext().execute("DuckDB filter demo", new WayangPlan(sink));

        printQuery("SELECT order_id, region, amount FROM " + filterResult + " ORDER BY order_id");
    }

    private void runProjectionPushdown() throws Exception {
        System.out.println();
        System.out.println("DuckDB demo: Projection + filter pushdown");
        System.out.println("SQL shape: SELECT region, amount FROM " + orders + " WHERE region = 'AMER'");

        DuckDBTableSource source = new DuckDBTableSource(
                orders, "order_id", "customer_id", "region", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record record) -> "AMER".equals(record.getField(2)), Record.class)
                        .withSqlImplementation("region = 'AMER'"));
        MapOperator<Record, Record> projection = new MapOperator<>(
                ProjectionDescriptor.createForRecords(
                        new RecordType("order_id", "customer_id", "region", "amount"),
                        "region", "amount"),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = new TableSink<>(
                new Properties(), "overwrite", projectionResult,
                "region", "amount");

        source.connectTo(0, filter, 0);
        filter.connectTo(0, projection, 0);
        projection.connectTo(0, sink, 0);
        wayangContext().execute("DuckDB projection demo", new WayangPlan(sink));

        printQuery("SELECT region, amount FROM " + projectionResult + " ORDER BY amount DESC");
    }

    private WayangContext wayangContext() {
        return new WayangContext(configuration)
                .withPlugin(DuckDB.plugin());
    }

    private void createFixture() throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS wayang_demo");
            statement.execute("CREATE TABLE " + orders + " ("
                    + "order_id BIGINT, customer_id BIGINT, region VARCHAR, amount DOUBLE)");
            statement.execute("INSERT INTO " + orders + " VALUES "
                    + "(1, 100, 'AMER', 2200.0),"
                    + "(2, 101, 'EMEA',  800.5),"
                    + "(3, 100, 'AMER',  680.5),"
                    + "(4, 102, 'APAC', 1500.0),"
                    + "(5, 101, 'EMEA', 1100.0),"
                    + "(6, 100, 'AMER',  950.25)");
        }
    }

    private void printQuery(String sql) throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            int columns = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                StringBuilder row = new StringBuilder("  ");
                for (int i = 1; i <= columns; i++) {
                    if (i > 1) {
                        row.append(" | ");
                    }
                    row.append(resultSet.getObject(i));
                }
                System.out.println(row);
            }
        }
    }
}
