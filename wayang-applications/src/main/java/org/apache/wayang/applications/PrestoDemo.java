/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.Java;
import org.apache.wayang.presto.Presto;
import org.apache.wayang.presto.operators.PrestoTableSource;

import java.util.ArrayList;
import java.util.List;

/** Configurable filter and projection example for the Wayang Presto connector. */
public class PrestoDemo {

    public static void main(String[] args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: PrestoDemo <configuration URL>");
        }

        Configuration configuration = new Configuration(args[0]);
        configuration.getStringProperty("wayang.presto.jdbc.url");
        String table = configuration.getStringProperty(
                "wayang.presto.demo.table", "memory.default.orders"
        );

        WayangContext wayang = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Presto.plugin());
        List<Record> results = new ArrayList<>();

        PrestoTableSource source = new PrestoTableSource(
                table, "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        record -> "AMER".equals(record.getField(1)), Record.class
                ).withSqlImplementation("region = 'AMER'")
        );
        MapOperator<Record, Record> projection = new MapOperator<>(
                ProjectionDescriptor.createForRecords(
                        new RecordType("order_id", "region", "product", "amount", "order_date"),
                        "region", "product", "amount"
                ),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class)
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);

        source.connectTo(0, filter, 0);
        filter.connectTo(0, projection, 0);
        projection.connectTo(0, sink, 0);
        wayang.execute("Presto-Filter-Projection-Demo", new WayangPlan(sink));

        System.out.printf("%-8s %-16s %10s%n", "region", "product", "amount");
        for (Record result : results) {
            System.out.printf("%-8s %-16s %10s%n",
                    result.getField(0), result.getField(1), result.getField(2));
        }
        System.out.printf("%n%d AMER rows returned from %s.%n", results.size(), table);
    }
}
