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

package org.apache.wayang.tests;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.ParquetSource;
import org.apache.wayang.bigquery.BigQuery;
import org.apache.wayang.bigquery.operators.BigQueryJoinOperator;
import org.apache.wayang.bigquery.operators.BigQueryParquetSource;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.jdbc.operators.JdbcParquetSource;
import org.apache.wayang.presto.Presto;
import org.apache.wayang.presto.operators.PrestoJoinOperator;
import org.apache.wayang.presto.operators.PrestoParquetSource;
import org.apache.wayang.trino.Trino;
import org.apache.wayang.trino.operators.TrinoJoinOperator;
import org.apache.wayang.trino.operators.TrinoParquetSource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for Parquet-backed SQL sources when several SQL platforms
 * are registered in the same {@link WayangContext}.
 */
class SqlParquetMultiPlatformIT {

    private static final String ORDERS_PARQUET = "s3://wayang-profiler/orders.parquet";
    private static final String CUSTOMERS_PARQUET = "s3://wayang-profiler/customers.parquet";

    @Test
    void registersParquetJoinAlternativesForAllSqlPlatforms() {
        WayangContext context = new WayangContext()
                .withPlugin(Trino.plugin())
                .withPlugin(Presto.plugin())
                .withPlugin(BigQuery.plugin());
        Configuration configuration = context.getConfiguration();

        WayangPlan plan = createParquetJoinPlan();
        plan.prepare();
        plan.applyTransformations(gatherTransformations(configuration));

        Collection<OperatorAlternative> alternatives = PlanTraversal.upstream()
                .traverse(plan.getSinks())
                .getTraversedNodesWith(operator -> operator instanceof OperatorAlternative)
                .stream()
                .map(operator -> (OperatorAlternative) operator)
                .collect(Collectors.toList());

        List<OperatorAlternative> sourceAlternatives = alternatives.stream()
                .filter(alternative -> containsAlternative(alternative, ParquetSource.class))
                .collect(Collectors.toList());
        List<OperatorAlternative> joinAlternatives = alternatives.stream()
                .filter(alternative -> containsAlternative(alternative, JoinOperator.class))
                .collect(Collectors.toList());

        assertEquals(2, sourceAlternatives.size());
        sourceAlternatives.forEach(alternative -> {
            assertTrue(containsAlternative(alternative, TrinoParquetSource.class));
            assertTrue(containsAlternative(alternative, PrestoParquetSource.class));
            assertTrue(containsAlternative(alternative, BigQueryParquetSource.class));
        });

        assertEquals(1, joinAlternatives.size());
        OperatorAlternative joinAlternative = joinAlternatives.get(0);
        assertTrue(containsAlternative(joinAlternative, TrinoJoinOperator.class));
        assertTrue(containsAlternative(joinAlternative, PrestoJoinOperator.class));
        assertTrue(containsAlternative(joinAlternative, BigQueryJoinOperator.class));
    }

    @Test
    void resolvesSameParquetUriToPlatformSpecificRelations() {
        Configuration configuration = new Configuration();
        configuration.setProperty(
                "wayang.trino.parquetsource.mappings",
                ORDERS_PARQUET + "=iceberg.profiler.orders"
        );
        configuration.setProperty(
                "wayang.presto.parquetsource.mappings",
                ORDERS_PARQUET + "=hive.profiler.orders"
        );
        configuration.setProperty(
                "wayang.bigquery.parquetsource.mappings",
                ORDERS_PARQUET + "=`project.profiler.orders_ext`"
        );

        assertEquals(
                "iceberg.profiler.orders",
                JdbcParquetSource.resolveSourceName(configuration, "trino", ORDERS_PARQUET)
        );
        assertEquals(
                "hive.profiler.orders",
                JdbcParquetSource.resolveSourceName(configuration, "presto", ORDERS_PARQUET)
        );
        assertEquals(
                "`project.profiler.orders_ext`",
                JdbcParquetSource.resolveSourceName(configuration, "bigquery", ORDERS_PARQUET)
        );
    }

    private static WayangPlan createParquetJoinPlan() {
        ParquetSource orders = new ParquetSource(
                ORDERS_PARQUET,
                null,
                "order_id",
                "customer_id",
                "amount"
        );
        ParquetSource customers = new ParquetSource(
                CUSTOMERS_PARQUET,
                null,
                "customer_id",
                "region"
        );

        JoinOperator<Record, Record, Integer> join = new JoinOperator<>(
                new TransformationDescriptor<Record, Integer>(
                        record -> (Integer) record.getField(1),
                        Record.class,
                        Integer.class
                ).withSqlImplementation(ORDERS_PARQUET, "customer_id"),
                new TransformationDescriptor<Record, Integer>(
                        record -> (Integer) record.getField(0),
                        Record.class,
                        Integer.class
                ).withSqlImplementation(CUSTOMERS_PARQUET, "customer_id")
        );

        @SuppressWarnings({"rawtypes", "unchecked"})
        DataSetType<Tuple2<Record, Record>> joinOutputType =
                (DataSetType) DataSetType.createDefaultUnchecked(Tuple2.class);
        List<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink =
                LocalCallbackSink.createCollectingSink(collector, joinOutputType);

        orders.connectTo(0, join, 0);
        customers.connectTo(0, join, 1);
        join.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }

    private static Collection<PlanTransformation> gatherTransformations(Configuration configuration) {
        Set<Platform> platforms = WayangCollections.asSet(configuration.getPlatformProvider().provideAll());
        return configuration.getMappingProvider().provideAll().stream()
                .flatMap((Mapping mapping) -> mapping.getTransformations().stream())
                .filter(transformation -> transformation.getTargetPlatforms().isEmpty()
                        || platforms.containsAll(transformation.getTargetPlatforms()))
                .collect(Collectors.toList());
    }

    private static boolean containsAlternative(OperatorAlternative alternative,
                                               Class<? extends Operator> operatorClass) {
        return alternative.getAlternatives().stream()
                .map(OperatorAlternative.Alternative::getContainedOperator)
                .anyMatch(operatorClass::isInstance);
    }
}
