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
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.bigquery.BigQuery;
import org.apache.wayang.bigquery.operators.BigQueryTableSource;
import org.apache.wayang.bigquery.platform.BigQueryPlatform;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.Java;

import java.util.ArrayList;
import java.util.List;

/**
 * Configurable example for the Wayang BigQuery connector.
 *
 * <p>The optional second argument selects a mode:
 * <ul>
 *   <li>{@code cost}: cost model output (no credentials needed)</li>
 *   <li>{@code filter}: filter operator pushdown</li>
 *   <li>{@code projection}: projection and filter operator pushdown</li>
 * </ul>
 *
 * <p>See {@code wayang-applications/bigquery.md} for configuration and usage.
 */
public class BigQueryDemo {

    private static Configuration configuration;
    private static String jdbcUrl;
    private static String sourceTable;

    public static void main(String[] args) {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException(
                    "Usage: BigQueryDemo <configuration URL> [cost|filter|projection|all]"
            );
        }
        configuration = new Configuration(args[0]);
        jdbcUrl = configuration.getStringProperty("wayang.bigquery.jdbc.url", "");
        sourceTable = configuration.getStringProperty(
                "wayang.bigquery.demo.table", "`my-project.sales.orders`"
        );
        String mode = args.length == 2 ? args[1] : "all";
        switch (mode) {
            case "cost":       costModel();    break;
            case "filter":     filterDemo();   break;
            case "projection": projectionDemo(); break;
            case "all":
                costModel();
                filterDemo();
                projectionDemo();
                break;
            default:
                throw new IllegalArgumentException("Unknown BigQuery demo mode: " + mode);
        }
    }

    static void costModel() {
        BigQueryPlatform.getInstance().configureDefaults(configuration);

        long   mhz   = configuration.getLongProperty("wayang.bigquery.cpu.mhz",        0);
        long   cores = configuration.getLongProperty("wayang.bigquery.cores",           0);
        double fix   = configuration.getDoubleProperty("wayang.bigquery.costs.fix",     0);
        double perMs = configuration.getDoubleProperty("wayang.bigquery.costs.per-ms",  1);

        long   rows      = 10;
        long   alpha     = 5;
        long   beta      = 2_000_000;
        long   cpuCycles = alpha * rows + beta;
        double timeMs    = cpuCycles / (cores * mhz * 1000.0);
        double cost      = fix + perMs * timeMs;

        System.out.println();
        System.out.println("BigQuery cost model");
        System.out.println();
        System.out.println("  Layer 1: cost formula (wayang-bigquery-defaults.properties)");
        System.out.printf("    tablesource : %s%n", configuration.getStringProperty("wayang.bigquery.tablesource.load", null));
        System.out.printf("    filter      : %s%n", configuration.getStringProperty("wayang.bigquery.filter.load",      null));
        System.out.println();
        System.out.println("  Layer 2: hardware profile (CPU cycles to wall-clock time)");
        System.out.printf("    cpu.mhz = %d   cores = %d%n", mhz, cores);
        System.out.println();
        System.out.println("  Layer 3: time to abstract cost");
        System.out.printf("    costs.fix = %.1f   costs.per-ms = %.1f%n", fix, perMs);
        System.out.println();
        System.out.println("  -- Worked example: 10-row table scan --");
        System.out.printf("    alpha = %d  (per-row, serverless columnar)%n", alpha);
        System.out.printf("    beta  = %,d  (cold-start / slot reservation)%n", beta);
        System.out.printf("    cpu cycles = %d * %d + %,d = %,d%n", alpha, rows, beta, cpuCycles);
        System.out.printf("    time       = %,d / (%d * %d * 1000) = %.4f ms%n", cpuCycles, cores, mhz, timeMs);
        System.out.printf("    cost       = %.1f + %.1f * %.4f = %.4f%n", fix, perMs, timeMs, cost);
        System.out.println();
        System.out.println();
    }

    static void filterDemo() {
        requireJdbcUrl();
        String table = sourceTable;

        System.out.println();
        System.out.println("BigQuery filter pushdown");
        System.out.println();
        System.out.println("  Operator:  FilterOperator  ->  BigQueryFilterOperator");
        System.out.printf("  SQL sent:  SELECT * FROM %s%n", table);
        System.out.println("                      WHERE region = 'AMER'");
        System.out.println();

        runLiveFilter(table);

        System.out.println();
    }

    private static void runLiveFilter(String table) {
        WayangContext wayang = buildWayang();
        List<Record> results = new ArrayList<>();

        BigQueryTableSource source = new BigQueryTableSource(
                table, "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "AMER".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'AMER'")
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);
        wayang.execute("BigQuery-Filter-Demo", new WayangPlan(sink));

        System.out.println("  Results returned by Wayang:");
        System.out.printf("  %-10s %-6s %-10s %10s %-12s%n",
                "order_id", "region", "product", "amount", "order_date");
        System.out.println("  " + repeat('-', 54));
        for (Record r : results) {
            System.out.printf("  %-10s %-6s %-10s %10s %-12s%n",
                    r.getField(0), r.getField(1), r.getField(2), r.getField(3), r.getField(4));
        }
        System.out.println();
        System.out.printf("  %d AMER rows returned.%n%n", results.size());
    }

    static void projectionDemo() {
        requireJdbcUrl();
        String table = sourceTable;

        System.out.println();
        System.out.println("BigQuery projection pushdown");
        System.out.println();
        System.out.println("  Operators: FilterOperator  ->  BigQueryFilterOperator");
        System.out.println("             MapOperator     ->  BigQueryProjectionOperator");
        System.out.printf("  SQL sent:  SELECT region, product, amount%n");
        System.out.printf("             FROM %s%n", table);
        System.out.println("             WHERE region = 'AMER'");
        System.out.println();
        System.out.println("  Both operators are combined in one SQL query; only 3 of 5");
        System.out.println("  columns transferred; order_id + order_date never leave BQ.");
        System.out.println();

        runLiveProjection(table);

        System.out.println();
    }

    private static void runLiveProjection(String table) {
        WayangContext wayang = buildWayang();
        List<Record> results = new ArrayList<>();

        BigQueryTableSource source = new BigQueryTableSource(
                table, "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "AMER".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'AMER'")
        );
        // Use the Record-specific descriptor for a projection with multiple fields.
        MapOperator<Record, Record> projection = new MapOperator<>(
                ProjectionDescriptor.createForRecords(
                        new RecordType("order_id", "region", "product", "amount", "order_date"),
                        "region", "product", "amount"),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class)
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, projection, 0);
        projection.connectTo(0, sink, 0);
        wayang.execute("BigQuery-Projection-Demo", new WayangPlan(sink));

        System.out.println("  Results returned by Wayang (projected columns only):");
        System.out.printf("  %-6s %-10s %10s%n", "region", "product", "amount");
        System.out.println("  " + repeat('-', 30));
        for (Record r : results) {
            System.out.printf("  %-6s %-10s %10s%n", r.getField(0), r.getField(1), r.getField(2));
        }
        System.out.println();
        System.out.printf("  %d AMER rows returned with 3 columns.%n%n",
                results.size());
    }

    private static WayangContext buildWayang() {
        return new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(BigQuery.plugin());
    }

    private static void requireJdbcUrl() {
        if (jdbcUrl.isEmpty()) {
            throw new IllegalArgumentException(
                    "wayang.bigquery.jdbc.url is required for filter and projection modes."
            );
        }
    }

    private static String repeat(char c, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) sb.append(c);
        return sb.toString();
    }
}
