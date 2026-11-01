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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.wayang.duckdb;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.basic.operators.SortOperator;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.duckdb.operators.DuckDBProjectionOperator;
import org.apache.wayang.duckdb.operators.DuckDBTableSource;
import org.apache.wayang.duckdb.platform.DuckDBPlatform;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Small DuckDB cost-profiling pilot.
 */
class DuckDBCostPilotIT {

    private static final String SCHEMA = "wayang_profile";
    private static final String CUSTOMERS_1K = SCHEMA + ".customers_1k";
    private static final int[] ROW_COUNTS = parseIntList(System.getProperty(
            "duckdb.profile.rowCounts",
            "10000,50000,100000,250000"
    ));
    private static final String[] COLUMNS = {"order_id", "customer_id", "region", "amount", "bucket"};
    private static final String[] JOIN_COLUMNS = {
            "order_id", "customer_id", "region", "amount", "bucket", "cust_id", "tier"
    };
    private static final String[] JOIN_ORDER_TIER_AMOUNT_COLUMNS = {"order_id", "tier", "amount"};
    private static final String[] JOIN_TIER_AMOUNT_COLUMNS = {"tier", "amount"};
    private static final String JOIN_FLATTEN_NAME = "DuckDB profile join flatten";
    private static final String JOIN_ORDER_TIER_AMOUNT_FLATTEN_NAME = "DuckDB profile join order tier amount flatten";
    private static final String JOIN_TIER_AMOUNT_FLATTEN_NAME = "DuckDB profile join tier amount flatten";
    private static final Path OUTPUT_DIR = Paths.get(System.getProperty(
            "duckdb.profile.outputDir",
            "target/cost-profiling/duckdb"
    ));
    private static final Path EXECUTIONS_PATH = OUTPUT_DIR.resolve("executions.json");
    private static final Path CARDINALITIES_PATH = OUTPUT_DIR.resolve("cardinalities.json");
    private static final Path MANIFEST_PATH = OUTPUT_DIR.resolve("manifest.csv");
    private static final List<String> PLAN_IDS = Arrays.asList(
            System.getProperty(
                    "duckdb.profile.plans",
                    "S01,S02,S03,S04,S05,S06,S07,S08,S09,S10,S11,S12,S13"
            ).split(",")
    );
    private static final int REPETITIONS = Integer.parseInt(
            System.getProperty("duckdb.profile.repetitions", "6")
    );
    private static final boolean RESET_OUTPUT = Boolean.parseBoolean(
            System.getProperty("duckdb.profile.reset", "true")
    );

    private static Path databaseFile;
    private static String jdbcUrl;

    @AfterEach
    void cleanUpDatabaseFile() throws Exception {
        if (databaseFile != null) {
            Files.deleteIfExists(databaseFile);
            Files.deleteIfExists(databaseFile.resolveSibling(databaseFile.getFileName() + ".wal"));
            databaseFile = null;
        }
    }

    @Test
    void runPilot() throws Exception {
        openFreshDatabase();
        Files.createDirectories(OUTPUT_DIR);
        initializeOutputFiles();

        prepareTables();

        for (int rowCount : ROW_COUNTS) {
            for (String planId : PLAN_IDS) {
                String normalizedPlanId = planId.trim();
                runPlan(
                        normalizedPlanId,
                        getOperatorChain(normalizedPlanId),
                        rowCount,
                        getExpectedRows(normalizedPlanId, rowCount)
                );
            }
        }

        assertTrue(Files.exists(EXECUTIONS_PATH), "execution log should be written");
        if (Files.exists(CARDINALITIES_PATH)) {
            assertFalse(Files.readAllLines(CARDINALITIES_PATH).isEmpty(), "cardinality log should contain rows");
        }
        assertTrue(Files.exists(MANIFEST_PATH), "manifest should be written");
        assertFalse(Files.readAllLines(MANIFEST_PATH).isEmpty(), "manifest should contain rows");
    }

    private void runPlan(String planId, String operatorChain, int rowCount, long expectedRows) throws Exception {
        for (int repetition = 0; repetition < REPETITIONS; repetition++) {
            boolean isWarmup = repetition == 0;
            String runId = String.format("%s_%s_r%02d", planId, formatRows(rowCount), repetition);
            String sourceTable = SCHEMA + ".orders_" + formatRows(rowCount);
            String sinkTable = SCHEMA + ".sink_" + runId.toLowerCase();

            dropTable(sinkTable);
            WayangPlan plan = createPlan(planId, sourceTable, sinkTable);
            wayangContext().execute(runId, plan);

            long actualRows = queryLong("SELECT count(*) FROM " + sinkTable);
            assertEquals(expectedRows, actualRows, runId + " row count");
            appendManifest(runId, planId, operatorChain, rowCount, expectedRows, repetition, isWarmup, sinkTable);
            dropTable(sinkTable);
        }
    }

    private WayangPlan createPlan(String planId, String sourceTable, String sinkTable) {
        DuckDBTableSource source = new DuckDBTableSource(sourceTable, COLUMNS);

        if ("S01".equals(planId)) {
            TableSink<Record> sink = new TableSink<>(new Properties(), "overwrite", sinkTable, COLUMNS);
            source.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S02".equals(planId)) {
            TableSink<Record> sink = new TableSink<>(new Properties(), "overwrite", sinkTable, COLUMNS);
            FilterOperator<Record> filter = createAmerFilter();
            source.connectTo(0, filter, 0);
            filter.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S03".equals(planId)) {
            TableSink<Record> sink = new TableSink<>(
                    new Properties(), "overwrite", sinkTable, "order_id", "amount");
            MapOperator<Record, Record> projection = createOrderAmountProjection();
            source.connectTo(0, projection, 0);
            projection.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S04".equals(planId)) {
            TableSink<Record> sink = new TableSink<>(
                    new Properties(), "overwrite", sinkTable, "order_id", "amount");
            FilterOperator<Record> filter = createAmerFilter();
            MapOperator<Record, Record> projection = createOrderAmountProjection();
            source.connectTo(0, filter, 0);
            filter.connectTo(0, projection, 0);
            projection.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S05".equals(planId)) {
            TableSink<Record> sink = new TableSink<>(
                    new Properties(), "overwrite", sinkTable, "total_amount");
            GlobalReduceOperator<Record> reduce = createGlobalAmountReduceOperator();
            source.connectTo(0, reduce, 0);
            reduce.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S06".equals(planId)) {
            TableSink<Record> sink = new TableSink<>(
                    new Properties(), "overwrite", sinkTable, "bucket", "total_amount");
            ReduceByOperator<Record, Record> reduceBy = createBucketReduceByOperator();
            source.connectTo(0, reduceBy, 0);
            reduceBy.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S07".equals(planId)) {
            TableSink<Record> sink = new TableSink<>(new Properties(), "overwrite", sinkTable, COLUMNS);
            SortOperator<Record, Record> sort = createAmountSortOperator(3);
            source.connectTo(0, sort, 0);
            sort.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S08".equals(planId)) {
            DuckDBTableSource customers = new DuckDBTableSource(CUSTOMERS_1K, "cust_id", "tier");
            JoinOperator<Record, Record, Record> join = createCustomerJoinOperator(sourceTable);
            MapOperator<Tuple2<Record, Record>, Record> flatten = createJoinFlattenOperator();
            TableSink<Record> sink = new TableSink<>(
                    new Properties(), "overwrite", sinkTable, JOIN_COLUMNS);
            source.connectTo(0, join, 0);
            customers.connectTo(0, join, 1);
            join.connectTo(0, flatten, 0);
            flatten.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S09".equals(planId)) {
            TableSink<Record> sink = new TableSink<>(
                    new Properties(), "overwrite", sinkTable, "total_amount");
            FilterOperator<Record> filter = createAmerFilter();
            GlobalReduceOperator<Record> reduce = createGlobalAmountReduceOperator();
            source.connectTo(0, filter, 0);
            filter.connectTo(0, reduce, 0);
            reduce.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S10".equals(planId)) {
            TableSink<Record> sink = new TableSink<>(
                    new Properties(), "overwrite", sinkTable, "bucket", "total_amount");
            FilterOperator<Record> filter = createAmerFilter();
            ReduceByOperator<Record, Record> reduceBy = createBucketReduceByOperator();
            source.connectTo(0, filter, 0);
            filter.connectTo(0, reduceBy, 0);
            reduceBy.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S11".equals(planId)) {
            TableSink<Record> sink = new TableSink<>(new Properties(), "overwrite", sinkTable, COLUMNS);
            FilterOperator<Record> filter = createAmerFilter();
            SortOperator<Record, Record> sort = createAmountSortOperator(3);
            source.connectTo(0, filter, 0);
            filter.connectTo(0, sort, 0);
            sort.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S12".equals(planId)) {
            TableSink<Record> sink = new TableSink<>(
                    new Properties(), "overwrite", sinkTable, "order_id", "amount");
            MapOperator<Record, Record> projection = createOrderAmountProjection();
            SortOperator<Record, Record> sort = createAmountSortOperator(1);
            source.connectTo(0, projection, 0);
            projection.connectTo(0, sort, 0);
            sort.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S13".equals(planId)) {
            TableSink<Record> sink = new TableSink<>(
                    new Properties(), "overwrite", sinkTable, "order_id", "amount");
            FilterOperator<Record> filter = createAmerFilter();
            MapOperator<Record, Record> projection = createOrderAmountProjection();
            SortOperator<Record, Record> sort = createAmountSortOperator(1);
            source.connectTo(0, filter, 0);
            filter.connectTo(0, projection, 0);
            projection.connectTo(0, sort, 0);
            sort.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S14".equals(planId)) {
            DuckDBTableSource customers = new DuckDBTableSource(CUSTOMERS_1K, "cust_id", "tier");
            FilterOperator<Record> filter = createAmerFilter();
            JoinOperator<Record, Record, Record> join = createCustomerJoinOperator(sourceTable);
            MapOperator<Tuple2<Record, Record>, Record> flatten = createJoinFlattenOperator();
            TableSink<Record> sink = new TableSink<>(
                    new Properties(), "overwrite", sinkTable, JOIN_COLUMNS);
            source.connectTo(0, filter, 0);
            filter.connectTo(0, join, 0);
            customers.connectTo(0, join, 1);
            join.connectTo(0, flatten, 0);
            flatten.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S15".equals(planId)) {
            DuckDBTableSource customers = new DuckDBTableSource(CUSTOMERS_1K, "cust_id", "tier");
            JoinOperator<Record, Record, Record> join = createCustomerJoinOperator(sourceTable);
            MapOperator<Tuple2<Record, Record>, Record> flatten = createJoinOrderTierAmountFlattenOperator();
            SortOperator<Record, Record> sort = createAmountSortOperator(2);
            TableSink<Record> sink = new TableSink<>(
                    new Properties(), "overwrite", sinkTable, JOIN_ORDER_TIER_AMOUNT_COLUMNS);
            source.connectTo(0, join, 0);
            customers.connectTo(0, join, 1);
            join.connectTo(0, flatten, 0);
            flatten.connectTo(0, sort, 0);
            sort.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        if ("S16".equals(planId)) {
            DuckDBTableSource customers = new DuckDBTableSource(CUSTOMERS_1K, "cust_id", "tier");
            JoinOperator<Record, Record, Record> join = createCustomerJoinOperator(sourceTable);
            MapOperator<Tuple2<Record, Record>, Record> flatten = createJoinTierAmountFlattenOperator();
            ReduceByOperator<Record, Record> reduceBy = createTierReduceByOperator();
            TableSink<Record> sink = new TableSink<>(
                    new Properties(), "overwrite", sinkTable, "tier", "total_amount");
            source.connectTo(0, join, 0);
            customers.connectTo(0, join, 1);
            join.connectTo(0, flatten, 0);
            flatten.connectTo(0, reduceBy, 0);
            reduceBy.connectTo(0, sink, 0);
            return new WayangPlan(sink);
        }

        throw new IllegalArgumentException("Unsupported pilot plan: " + planId);
    }

    private static GlobalReduceOperator<Record> createGlobalAmountReduceOperator() {
        return new GlobalReduceOperator<>(
                new ReduceDescriptor<>((left, right) -> left, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount"),
                DataSetType.createDefault(Record.class));
    }

    private static ReduceByOperator<Record, Record> createBucketReduceByOperator() {
        return new ReduceByOperator<>(
                new TransformationDescriptor<>(
                        record -> new Record(record.getField(4)),
                        Record.class,
                        Record.class
                ).withSqlImplementation("bucket", "bucket"),
                new ReduceDescriptor<>((left, right) -> left, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount"),
                DataSetType.createDefault(Record.class));
    }

    private static ReduceByOperator<Record, Record> createTierReduceByOperator() {
        return new ReduceByOperator<>(
                new TransformationDescriptor<>(
                        record -> new Record(record.getField(0)),
                        Record.class,
                        Record.class
                ).withSqlImplementation("tier", "tier"),
                new ReduceDescriptor<>((left, right) -> left, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount"),
                DataSetType.createDefault(Record.class));
    }

    private static SortOperator<Record, Record> createAmountSortOperator(int amountFieldIndex) {
        return new SortOperator<>(
                new TransformationDescriptor<>(
                        record -> new Record(record.getField(amountFieldIndex)),
                        Record.class,
                        Record.class
                ).withSqlImplementation("amount", "ASC"),
                DataSetType.createDefault(Record.class));
    }

    private static JoinOperator<Record, Record, Record> createCustomerJoinOperator(String sourceTable) {
        return new JoinOperator<>(
                new TransformationDescriptor<>(
                        record -> new Record(record.getField(1)),
                        Record.class,
                        Record.class
                ).withSqlImplementation(sourceTable, "customer_id"),
                new TransformationDescriptor<>(
                        record -> new Record(record.getField(0)),
                        Record.class,
                        Record.class
                ).withSqlImplementation(CUSTOMERS_1K, "cust_id"));
    }

    private static FilterOperator<Record> createAmerFilter() {
        return new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record record) -> "AMER".equals(record.getField(2)),
                        Record.class
                ).withSqlImplementation("region = 'AMER'")
        );
    }

    private static MapOperator<Record, Record> createOrderAmountProjection() {
        return new MapOperator<>(
                ProjectionDescriptor.createForRecords(
                        new RecordType(COLUMNS),
                        "order_id", "amount"),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class));
    }

    private static MapOperator<Tuple2<Record, Record>, Record> createJoinFlattenOperator() {
        return createJoinFlattenOperator(new JoinFlattenFunction(), JOIN_FLATTEN_NAME);
    }

    private static MapOperator<Tuple2<Record, Record>, Record> createJoinOrderTierAmountFlattenOperator() {
        return createJoinFlattenOperator(new JoinOrderTierAmountFlattenFunction(), JOIN_ORDER_TIER_AMOUNT_FLATTEN_NAME);
    }

    private static MapOperator<Tuple2<Record, Record>, Record> createJoinTierAmountFlattenOperator() {
        return createJoinFlattenOperator(new JoinTierAmountFlattenFunction(), JOIN_TIER_AMOUNT_FLATTEN_NAME);
    }

    private static MapOperator<Tuple2<Record, Record>, Record> createJoinFlattenOperator(
            FunctionDescriptor.SerializableFunction<Tuple2<Record, Record>, Record> function,
            String name) {
        MapOperator<Tuple2<Record, Record>, Record> operator = new MapOperator<>(
                new TransformationDescriptor<>(
                        function,
                        DataUnitType.createBasicUnchecked(Tuple2.class),
                        DataUnitType.createBasic(Record.class)),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                DataSetType.createDefault(Record.class));
        operator.setName(name);
        return operator;
    }

    private WayangContext wayangContext() {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.duckdb.jdbc.url", jdbcUrl);
        configuration.setProperty("wayang.duckdb.jdbc.user", "");
        configuration.setProperty("wayang.duckdb.jdbc.password", "");
        configuration.setProperty("wayang.core.log.enabled", "true");
        configuration.setProperty("wayang.core.explain.enabled", "false");
        configuration.setProperty("wayang.core.log.executions", EXECUTIONS_PATH.toString().replace('\\', '/'));
        configuration.setProperty("wayang.core.log.cardinalities", CARDINALITIES_PATH.toString().replace('\\', '/'));
        configuration.getMappingProvider().addAllToWhitelist(
                Collections.singleton(new JoinFlattenMapping()));
        return new WayangContext(configuration).withPlugin(DuckDB.plugin());
    }

    private static void prepareTables() throws Exception {
        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
            for (int rowCount : ROW_COUNTS) {
                String table = SCHEMA + ".orders_" + formatRows(rowCount);
                statement.execute("DROP TABLE IF EXISTS " + table);
                statement.execute("CREATE TABLE " + table + " AS "
                        + "SELECT "
                        + "CAST(n AS BIGINT) AS order_id, "
                        + "CAST(n % 1000 AS BIGINT) AS customer_id, "
                        + "CASE WHEN n % 2 = 0 THEN 'AMER' ELSE 'EMEA' END AS region, "
                        + "CAST(n % 10000 AS DOUBLE) AS amount, "
                        + "CAST(n % 100 AS BIGINT) AS bucket "
                        + "FROM range(1, " + (rowCount + 1) + ") AS t(n)");
                assertEquals(rowCount, queryLong("SELECT count(*) FROM " + table), table + " row count");
                assertEquals(rowCount / 2, queryLong("SELECT count(*) FROM " + table + " WHERE region = 'AMER'"),
                        table + " AMER row count");
            }
            statement.execute("DROP TABLE IF EXISTS " + CUSTOMERS_1K);
            statement.execute("CREATE TABLE " + CUSTOMERS_1K + " AS "
                    + "SELECT "
                    + "CAST(n - 1 AS BIGINT) AS cust_id, "
                    + "CASE WHEN n % 2 = 0 THEN 'GOLD' ELSE 'SILVER' END AS tier "
                    + "FROM range(1, 1001) AS t(n)");
            assertEquals(1000, queryLong("SELECT count(*) FROM " + CUSTOMERS_1K), CUSTOMERS_1K + " row count");
        }
    }

    private static String formatRows(int rowCount) {
        if (rowCount % 1000 == 0) {
            return (rowCount / 1000) + "k";
        }
        return String.valueOf(rowCount);
    }

    private static void initializeOutputFiles() throws Exception {
        if (RESET_OUTPUT) {
            Files.deleteIfExists(EXECUTIONS_PATH);
            Files.deleteIfExists(CARDINALITIES_PATH);
            writeManifestHeader();
        } else if (!Files.exists(MANIFEST_PATH)) {
            writeManifestHeader();
        }
    }

    private static void writeManifestHeader() throws Exception {
        try (BufferedWriter writer = Files.newBufferedWriter(MANIFEST_PATH, StandardCharsets.UTF_8)) {
            writer.write("run_id,plan_id,operator_chain,input_rows_left,input_rows_right,expected_output_rows,"
                    + "selectivity,repetition,is_warmup,sink_table,status,notes");
            writer.newLine();
        }
    }

    private static void appendManifest(
            String runId,
            String planId,
            String operatorChain,
            int inputRows,
            long expectedOutputRows,
            int repetition,
            boolean isWarmup,
            String sinkTable) throws Exception {
        try (BufferedWriter writer = Files.newBufferedWriter(
                MANIFEST_PATH,
                StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.APPEND)) {
            writer.write(String.join(",",
                    runId,
                    planId,
                    operatorChain,
                    String.valueOf(inputRows),
                    hasJoin(planId) ? "1000" : "",
                    String.valueOf(expectedOutputRows),
                    hasFilter(planId) ? "0.5" : "1.0",
                    String.valueOf(repetition),
                    String.valueOf(isWarmup),
                    sinkTable,
                    "ok",
                    ""));
            writer.newLine();
        }
    }

    private static String getOperatorChain(String planId) {
        switch (planId) {
            case "S01":
                return "TableSource->TableSink";
            case "S02":
                return "TableSource->Filter(50%)->TableSink";
            case "S03":
                return "TableSource->Projection->TableSink";
            case "S04":
                return "TableSource->Filter(50%)->Projection->TableSink";
            case "S05":
                return "TableSource->GlobalReduce->TableSink";
            case "S06":
                return "TableSource->ReduceBy(bucket)->TableSink";
            case "S07":
                return "TableSource->Sort(amount)->TableSink";
            case "S08":
                return "Orders->Join(Customers 1k)->Projection->TableSink";
            case "S09":
                return "TableSource->Filter(50%)->GlobalReduce->TableSink";
            case "S10":
                return "TableSource->Filter(50%)->ReduceBy(bucket)->TableSink";
            case "S11":
                return "TableSource->Filter(50%)->Sort(amount)->TableSink";
            case "S12":
                return "TableSource->Projection(order_id,amount)->Sort(amount)->TableSink";
            case "S13":
                return "TableSource->Filter(50%)->Projection(order_id,amount)->Sort(amount)->TableSink";
            case "S14":
                return "Orders->Filter(50%)->Join(Customers 1k)->Projection->TableSink";
            case "S15":
                return "Orders->Join(Customers 1k)->Projection(order_id,tier,amount)->Sort(amount)->TableSink";
            case "S16":
                return "Orders->Join(Customers 1k)->Projection(tier,amount)->ReduceBy(tier)->TableSink";
            default:
                throw new IllegalArgumentException("Unsupported pilot plan: " + planId);
        }
    }

    private static long getExpectedRows(String planId, int rowCount) {
        if ("S05".equals(planId) || "S09".equals(planId)) {
            return 1;
        }
        if ("S06".equals(planId)) {
            return 100;
        }
        if ("S10".equals(planId)) {
            return 50;
        }
        if ("S16".equals(planId)) {
            return 2;
        }
        return hasFilter(planId) ? rowCount / 2 : rowCount;
    }

    private static boolean hasFilter(String planId) {
        return "S02".equals(planId)
                || "S04".equals(planId)
                || "S09".equals(planId)
                || "S10".equals(planId)
                || "S11".equals(planId)
                || "S13".equals(planId)
                || "S14".equals(planId);
    }

    private static boolean hasJoin(String planId) {
        return "S08".equals(planId)
                || "S14".equals(planId)
                || "S15".equals(planId)
                || "S16".equals(planId);
    }

    private static int[] parseIntList(String value) {
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(token -> !token.isEmpty())
                .mapToInt(Integer::parseInt)
                .toArray();
    }

    private static long queryLong(String sql) throws Exception {
        try (Connection connection = jdbc();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private static void dropTable(String table) throws Exception {
        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    private static Connection jdbc() throws Exception {
        return DriverManager.getConnection(jdbcUrl);
    }

    private static void openFreshDatabase() throws Exception {
        databaseFile = Files.createTempFile("wayang-duckdb-cost-", ".duckdb");
        Files.deleteIfExists(databaseFile);
        jdbcUrl = "jdbc:duckdb:" + databaseFile.toAbsolutePath();
    }

    private static Record flattenJoinResult(Object joinResult) {
        if (joinResult instanceof Record) {
            return (Record) joinResult;
        }
        Tuple2<?, ?> pair = (Tuple2<?, ?>) joinResult;
        Record left = (Record) pair.field0;
        Record right = (Record) pair.field1;
        return new Record(
                left.getField(0),
                left.getField(1),
                left.getField(2),
                left.getField(3),
                left.getField(4),
                right.getField(0),
                right.getField(1));
    }

    private static Record flattenJoinOrderTierAmountResult(Object joinResult) {
        if (joinResult instanceof Record) {
            Record record = (Record) joinResult;
            return new Record(record.getField(0), record.getField(6), record.getField(3));
        }
        Tuple2<?, ?> pair = (Tuple2<?, ?>) joinResult;
        Record left = (Record) pair.field0;
        Record right = (Record) pair.field1;
        return new Record(left.getField(0), right.getField(1), left.getField(3));
    }

    private static Record flattenJoinTierAmountResult(Object joinResult) {
        if (joinResult instanceof Record) {
            Record record = (Record) joinResult;
            return new Record(record.getField(6), record.getField(3));
        }
        Tuple2<?, ?> pair = (Tuple2<?, ?>) joinResult;
        Record left = (Record) pair.field0;
        Record right = (Record) pair.field1;
        return new Record(right.getField(1), left.getField(3));
    }

    private static final class JoinFlattenFunction implements
            FunctionDescriptor.SerializableFunction<Tuple2<Record, Record>, Record> {

        @Override
        public Record apply(Tuple2<Record, Record> tuple) {
            return flattenJoinResult(tuple);
        }
    }

    private static final class JoinOrderTierAmountFlattenFunction implements
            FunctionDescriptor.SerializableFunction<Tuple2<Record, Record>, Record> {

        @Override
        public Record apply(Tuple2<Record, Record> tuple) {
            return flattenJoinOrderTierAmountResult(tuple);
        }
    }

    private static final class JoinTierAmountFlattenFunction implements
            FunctionDescriptor.SerializableFunction<Tuple2<Record, Record>, Record> {

        @Override
        public Record apply(Tuple2<Record, Record> tuple) {
            return flattenJoinTierAmountResult(tuple);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final class JoinFlattenMapping implements Mapping {

        @Override
        public java.util.Collection<PlanTransformation> getTransformations() {
            OperatorPattern<MapOperator> pattern = new OperatorPattern(
                    "joinFlatten",
                    new MapOperator(null, DataSetType.none(), DataSetType.createDefault(Record.class)),
                    false)
                    .withAdditionalTest(operator -> isJoinFlattenName(((MapOperator) operator).getName()));

            ReplacementSubplanFactory factory = new ReplacementSubplanFactory.OfSingleOperators<MapOperator>(
                    (matchedOperator, epoch) -> createDuckDBProjection(matchedOperator.getName()).at(epoch));

            return Collections.singleton(new PlanTransformation(
                    SubplanPattern.createSingleton(pattern),
                    factory,
                    DuckDBPlatform.getInstance()));
        }

        private static DuckDBProjectionOperator createDuckDBProjection(String operatorName) {
            ProjectionDescriptor<Tuple2<Record, Record>, Record> descriptor = new ProjectionDescriptor<>(
                    getJoinFlattenFunction(operatorName),
                    Arrays.asList(getJoinFlattenColumns(operatorName)),
                    DataUnitType.createBasicUnchecked(Tuple2.class),
                    DataUnitType.createBasic(Record.class));
            MapOperator<Tuple2<Record, Record>, Record> projection = new MapOperator<>(
                    descriptor,
                    DataSetType.createDefaultUnchecked(Tuple2.class),
                    DataSetType.createDefault(Record.class));
            projection.setName(operatorName);
            return new DuckDBProjectionOperator((MapOperator<Record, Record>) (MapOperator) projection);
        }

        private static boolean isJoinFlattenName(String operatorName) {
            return JOIN_FLATTEN_NAME.equals(operatorName)
                    || JOIN_ORDER_TIER_AMOUNT_FLATTEN_NAME.equals(operatorName)
                    || JOIN_TIER_AMOUNT_FLATTEN_NAME.equals(operatorName);
        }

        private static String[] getJoinFlattenColumns(String operatorName) {
            if (JOIN_ORDER_TIER_AMOUNT_FLATTEN_NAME.equals(operatorName)) {
                return JOIN_ORDER_TIER_AMOUNT_COLUMNS;
            }
            if (JOIN_TIER_AMOUNT_FLATTEN_NAME.equals(operatorName)) {
                return JOIN_TIER_AMOUNT_COLUMNS;
            }
            return JOIN_COLUMNS;
        }

        private static FunctionDescriptor.SerializableFunction<Tuple2<Record, Record>, Record> getJoinFlattenFunction(
                String operatorName) {
            if (JOIN_ORDER_TIER_AMOUNT_FLATTEN_NAME.equals(operatorName)) {
                return new JoinOrderTierAmountFlattenFunction();
            }
            if (JOIN_TIER_AMOUNT_FLATTEN_NAME.equals(operatorName)) {
                return new JoinTierAmountFlattenFunction();
            }
            return new JoinFlattenFunction();
        }
    }
}
