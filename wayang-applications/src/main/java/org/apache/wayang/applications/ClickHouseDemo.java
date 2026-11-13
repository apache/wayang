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
import org.apache.wayang.clickhouse.ClickHouse;
import org.apache.wayang.clickhouse.operators.ClickHouseTableSource;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.Java;

import java.util.ArrayList;
import java.util.List;

/**
 * Configurable example for the Wayang ClickHouse connector.
 *
 * <p>See {@code wayang-applications/clickhouse.md} for configuration and usage.
 */
public class ClickHouseDemo {

    private static Configuration configuration;
    private static String sourceTable;

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: ClickHouseDemo <configuration URL>");
        }
        configuration = new Configuration(args[0]);
        configuration.getStringProperty("wayang.clickhouse.jdbc.url");
        sourceTable = configuration.getStringProperty(
                "wayang.clickhouse.demo.table", "default.orders"
        );
        filterDemo();
        projectionDemo();
    }

    static void filterDemo() throws Exception {
        System.out.println("ClickHouse filter pushdown");
        System.out.println();
        System.out.println("  Operator:  FilterOperator  ->  ClickHouseFilterOperator");
        System.out.println("  SQL sent:  SELECT * FROM " + sourceTable);
        System.out.println("                      WHERE region = 'AMER'");
        System.out.println();

        WayangContext wayang = buildWayang();

        List<Record> results = new ArrayList<>();
        ClickHouseTableSource source = new ClickHouseTableSource(
                sourceTable, "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "AMER".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'AMER'")
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        wayang.execute("ClickHouse-Filter-Demo", new WayangPlan(sink));

        System.out.println("  Results returned by Wayang:");
        System.out.printf("  %-10s %-6s %-10s %10s %-12s%n",
                "order_id", "region", "product", "amount", "order_date");
        System.out.println("  " + repeat('-', 54));
        for (Record r : results) {
            System.out.printf("  %-10s %-6s %-10s %10s %-12s%n",
                    r.getField(0), r.getField(1), r.getField(2), r.getField(3), r.getField(4));
        }
        System.out.println();
        System.out.printf("  %d AMER rows returned.%n", results.size());
        System.out.println();
    }

    static void projectionDemo() throws Exception {
        System.out.println("ClickHouse projection pushdown");
        System.out.println();
        System.out.println("  Operators: FilterOperator  ->  ClickHouseFilterOperator");
        System.out.println("             MapOperator     ->  ClickHouseProjectionOperator");
        System.out.println("  SQL sent:  SELECT region, product, amount");
        System.out.println("             FROM " + sourceTable);
        System.out.println("             WHERE region = 'AMER'");
        System.out.println();

        WayangContext wayang = buildWayang();

        List<Record> results = new ArrayList<>();
        ClickHouseTableSource source = new ClickHouseTableSource(
                sourceTable, "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "AMER".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'AMER'")
        );
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

        wayang.execute("ClickHouse-Projection-Demo", new WayangPlan(sink));

        System.out.println("  Results returned by Wayang (projected columns only):");
        System.out.printf("  %-6s %-10s %10s%n", "region", "product", "amount");
        System.out.println("  " + repeat('-', 30));
        for (Record r : results) {
            System.out.printf("  %-6s %-10s %10s%n", r.getField(0), r.getField(1), r.getField(2));
        }
        System.out.println();
        System.out.printf("  %d AMER rows returned with 3 columns.%n", results.size());
        System.out.println();
    }

    private static WayangContext buildWayang() {
        return new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(ClickHouse.plugin());
    }

    private static String repeat(char c, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append(c);
        }
        return sb.toString();
    }
}
