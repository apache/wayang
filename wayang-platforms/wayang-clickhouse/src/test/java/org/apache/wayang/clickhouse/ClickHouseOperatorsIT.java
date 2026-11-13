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

package org.apache.wayang.clickhouse;

import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
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
import org.apache.wayang.clickhouse.operators.ClickHouseProjectionOperator;
import org.apache.wayang.clickhouse.operators.ClickHouseTableSource;
import org.apache.wayang.clickhouse.platform.ClickHousePlatform;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration tests for the ClickHouse platform against a live
 * ClickHouse server. The class is skipped (not failed) when the server is
 * unreachable.
 *
 * <p>Environment variables: {@code CLICKHOUSE_HOST} (default {@code localhost}),
 * {@code CLICKHOUSE_PORT} (default {@code 8123}), {@code CLICKHOUSE_USER}
 * (default {@code default}), {@code CLICKHOUSE_PASSWORD} (default empty).
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClickHouseOperatorsIT {

    private static final String HOST = System.getenv().getOrDefault("CLICKHOUSE_HOST", "localhost");
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("CLICKHOUSE_PORT", "8123"));
    private static final String USER = System.getenv().getOrDefault("CLICKHOUSE_USER", "default");
    private static final String PASSWORD = System.getenv().getOrDefault("CLICKHOUSE_PASSWORD", "");
    private static final String JDBC_URL = String.format("jdbc:clickhouse://%s:%d", HOST, PORT);

    private static final String DATABASE = "wayang_it";
    private static final String ORDERS = DATABASE + ".orders";
    private static final String CUSTOMERS = DATABASE + ".customers";
    private static final String SINK_TABLE_NAME = "operator_result";
    private static final String SINK_TABLE = DATABASE + "." + SINK_TABLE_NAME;
    private static final String[] JOIN_COLUMNS = {
            "order_id", "customer_id", "region", "amount", "cust_id", "name", "tier"
    };
    private static final String JOIN_FLATTEN_NAME = "ClickHouse test-only join flatten";

    private static boolean clickHouseAvailable = false;

    @BeforeAll
    static void setUp() {
        try (Connection c = jdbc()) {
            Statement st = c.createStatement();
            st.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE);

            st.execute("DROP TABLE IF EXISTS " + ORDERS);
            st.execute("CREATE TABLE " + ORDERS + " ("
                    + "order_id Int64, customer_id Int64, region String, amount Float64"
                    + ") ENGINE = MergeTree ORDER BY order_id");
            st.execute("INSERT INTO " + ORDERS + " VALUES "
                    + "(1, 100, 'AMER', 2200.0),"
                    + "(2, 101, 'EMEA',  800.5),"
                    + "(3, 100, 'AMER',  680.5),"
                    + "(4, 102, 'APAC', 1500.0),"
                    + "(5, 101, 'EMEA', 1100.0),"
                    + "(6, 100, 'AMER',  950.25)");
            // Scale to 120000 rows so the optimizer prefers SQL pushdown.
            st.execute("INSERT INTO " + ORDERS
                    + " SELECT order_id + (n * 10), customer_id, region, amount"
                    + " FROM " + ORDERS + " ARRAY JOIN range(1, 10000) AS n");
            st.execute("INSERT INTO " + ORDERS
                    + " SELECT order_id + 600000, customer_id, region, amount"
                    + " FROM " + ORDERS);

            st.execute("DROP TABLE IF EXISTS " + CUSTOMERS);
            st.execute("CREATE TABLE " + CUSTOMERS + " ("
                    + "cust_id Int64, name String, tier String"
                    + ") ENGINE = MergeTree ORDER BY cust_id");
            st.execute("INSERT INTO " + CUSTOMERS + " VALUES "
                    + "(100, 'Acme',   'GOLD'),"
                    + "(101, 'Globex', 'SILVER'),"
                    + "(102, 'Initech','BRONZE')");

            clickHouseAvailable = true;
            System.out.println("[ClickHouseOperatorsIT] Connected at " + JDBC_URL
                    + " — fixtures created.");
        } catch (Exception e) {
            System.err.println("[ClickHouseOperatorsIT] ClickHouse not available ("
                    + e.getMessage() + ") — skipping.");
        }
    }

    @AfterAll
    static void tearDown() {
        if (!clickHouseAvailable) {
            return;
        }
        try (Connection c = jdbc()) {
            Statement st = c.createStatement();
            st.execute("DROP TABLE IF EXISTS " + ORDERS);
            st.execute("DROP TABLE IF EXISTS " + CUSTOMERS);
            st.execute("DROP TABLE IF EXISTS " + SINK_TABLE);
        } catch (Exception e) {
            System.err.println("[ClickHouseOperatorsIT] cleanup failed: " + e.getMessage());
        }
    }

    @Test
    @Order(1)
    void tableSource() {
        Assumptions.assumeTrue(clickHouseAvailable, "ClickHouse not reachable");
        ClickHouseTableSource src = new ClickHouseTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount");
        TableSink<Record> sink = tableSink("order_id", "customer_id", "region", "amount");
        src.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(120000, queryLong("SELECT count(*) FROM " + SINK_TABLE),
                "TableSource should return all orders");
    }

    @Test
    @Order(2)
    void filter() {
        Assumptions.assumeTrue(clickHouseAvailable, "ClickHouse not reachable");
        ClickHouseTableSource src = new ClickHouseTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record r) -> "AMER".equals(r.getField(2)), Record.class
                ).withSqlImplementation("region = 'AMER'"));
        TableSink<Record> sink = tableSink("order_id", "customer_id", "region", "amount");
        src.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(60000, queryLong("SELECT count(*) FROM " + SINK_TABLE),
                "60000 AMER orders expected");
        assertEquals(0, queryLong("SELECT countIf(region != 'AMER') FROM " + SINK_TABLE),
                "all rows AMER");
    }

    @Test
    @Order(3)
    void projection() {
        Assumptions.assumeTrue(clickHouseAvailable, "ClickHouse not reachable");
        ClickHouseTableSource src = new ClickHouseTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record r) -> "AMER".equals(r.getField(2)), Record.class
                ).withSqlImplementation("region = 'AMER'"));
        MapOperator<Record, Record> projection = new MapOperator<>(
                ProjectionDescriptor.createForRecords(
                        new RecordType("order_id", "customer_id", "region", "amount"),
                        "region", "amount"),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = tableSink("region", "amount");
        src.connectTo(0, filter, 0);
        filter.connectTo(0, projection, 0);
        projection.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(60000, queryLong("SELECT count(*) FROM " + SINK_TABLE),
                "60000 AMER rows expected");
        assertEquals(2, queryLong(
                "SELECT count(*) FROM system.columns "
                        + "WHERE database = '" + DATABASE + "' AND table = '" + SINK_TABLE_NAME + "'"),
                "projection keeps only 2 columns");
    }

    @Test
    @Order(4)
    void join() {
        Assumptions.assumeTrue(clickHouseAvailable, "ClickHouse not reachable");
        ClickHouseTableSource orders = new ClickHouseTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount");
        ClickHouseTableSource customers = new ClickHouseTableSource(
                CUSTOMERS, "cust_id", "name", "tier");
        JoinOperator<Record, Record, Record> join = new JoinOperator<>(
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(1)), Record.class, Record.class
                ).withSqlImplementation(ORDERS, "customer_id"),
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(0)), Record.class, Record.class
                ).withSqlImplementation(CUSTOMERS, "cust_id"));
        MapOperator<Tuple2<Record, Record>, Record> flatten = joinFlattenOperator();
        TableSink<Record> sink = tableSink(JOIN_COLUMNS);
        orders.connectTo(0, join, 0);
        customers.connectTo(0, join, 1);
        join.connectTo(0, flatten, 0);
        flatten.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(120000, queryLong("SELECT count(*) FROM " + SINK_TABLE),
                "join should yield one row per order");
        assertEquals(0, queryLong("SELECT countIf(customer_id != cust_id) FROM " + SINK_TABLE),
                "joined customer IDs should match");
    }

    @Test
    @Order(5)
    void globalReduce() {
        Assumptions.assumeTrue(clickHouseAvailable, "ClickHouse not reachable");
        ClickHouseTableSource src = new ClickHouseTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount");
        GlobalReduceOperator<Record> reduce = new GlobalReduceOperator<>(
                new ReduceDescriptor<>((a, b) -> a, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount"),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = tableSink("total_amount");
        src.connectTo(0, reduce, 0);
        reduce.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertSingleDoubleResult(144_625_000.0, "global reduce must collapse to a single row");
    }

    @Test
    @Order(6)
    void reduceBy() {
        Assumptions.assumeTrue(clickHouseAvailable, "ClickHouse not reachable");
        ClickHouseTableSource src = new ClickHouseTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount");
        ReduceByOperator<Record, Record> reduceBy = new ReduceByOperator<>(
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(2)), Record.class, Record.class
                ).withSqlImplementation("region", "region"),
                new ReduceDescriptor<>((a, b) -> a, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount"),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = tableSink("region", "total_amount");
        src.connectTo(0, reduceBy, 0);
        reduceBy.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        Map<String, Double> sums = readRegionSums();
        assertEquals(3, sums.size(), "one row per region expected");
        assertEquals(76_615_000.0, sums.get("AMER"), 0.01);
        assertEquals(38_010_000.0, sums.get("EMEA"), 0.01);
        assertEquals(30_000_000.0, sums.get("APAC"), 0.01);
    }

    @Test
    @Order(7)
    void sort() {
        Assumptions.assumeTrue(clickHouseAvailable, "ClickHouse not reachable");
        ClickHouseTableSource src = new ClickHouseTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount");
        SortOperator<Record, Record> sort = new SortOperator<>(
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(3)), Record.class, Record.class
                ).withSqlImplementation("amount", "ASC"),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = tableSink("order_id", "customer_id", "region", "amount");
        src.connectTo(0, sort, 0);
        sort.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(120000, queryLong("SELECT count(*) FROM " + SINK_TABLE),
                "sort must not change the cardinality");
        assertEquals(680.5, queryDouble("SELECT min(amount) FROM " + SINK_TABLE), 0.001);
        assertEquals(2200.0, queryDouble("SELECT max(amount) FROM " + SINK_TABLE), 0.001);
    }

    @Test
    @Order(8)
    void tableSink() throws Exception {
        Assumptions.assumeTrue(clickHouseAvailable, "ClickHouse not reachable");

        ClickHouseTableSource src = new ClickHouseTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record r) -> "AMER".equals(r.getField(2)), Record.class
                ).withSqlImplementation("region = 'AMER'"));
        TableSink<Record> sink = new TableSink<>(
                new Properties(), "overwrite", SINK_TABLE,
                "order_id", "customer_id", "region", "amount");
        src.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        try (Connection c = jdbc()) {
            ResultSet rs = c.createStatement().executeQuery(
                    "SELECT count(*), countIf(region != 'AMER') FROM " + SINK_TABLE);
            rs.next();
            assertEquals(60000, rs.getLong(1), "sink table must hold all AMER orders");
            assertEquals(0, rs.getLong(2), "sink table must hold only AMER orders");
        }
    }

    @Test
    @Order(9)
    void javaPlanBuilderReadTableFilterProjection() {
        Assumptions.assumeTrue(clickHouseAvailable, "ClickHouse not reachable");

        new JavaPlanBuilder(
                wayangContext(), "ClickHouse JavaPlanBuilder readTable integration test")
                .readTable(new ClickHouseTableSource(
                        ORDERS, "order_id", "customer_id", "region", "amount"))
                .filter(record -> "AMER".equals(record.getField(2)))
                    .withSqlUdf("region = 'AMER'")
                .asRecords()
                .projectRecords(new String[]{"order_id", "amount"})
                .writeTable(SINK_TABLE, "overwrite", new String[]{"order_id", "amount"}, new Properties());

        assertEquals(60000, queryLong("SELECT count(*) FROM " + SINK_TABLE),
                "60000 projected AMER orders expected");
    }

    @Test
    @Order(10)
    void javaPlanBuilderReadTableJoin() {
        Assumptions.assumeTrue(clickHouseAvailable, "ClickHouse not reachable");

        JavaPlanBuilder plan = new JavaPlanBuilder(
                wayangContext(), "ClickHouse JavaPlanBuilder join integration test");
        DataQuantaBuilder<?, Record> orders = plan.readTable(new ClickHouseTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount"));
        DataQuantaBuilder<?, Record> customers = plan.readTable(new ClickHouseTableSource(
                CUSTOMERS, "cust_id", "name", "tier"));

        orders
                .join(
                        record -> new Record(record.getField(1)),
                        customers,
                        record -> new Record(record.getField(0)))
                    .withSqlUdfs(ORDERS, "customer_id", CUSTOMERS, "cust_id")
                .map(new JoinFlattenFunction())
                    .withName(JOIN_FLATTEN_NAME)
                .writeTable(SINK_TABLE, "overwrite", JOIN_COLUMNS, new Properties());

        assertEquals(120000, queryLong("SELECT count(*) FROM " + SINK_TABLE),
                "join should yield one row per order");
        assertEquals(0, queryLong("SELECT countIf(customer_id != cust_id) FROM " + SINK_TABLE),
                "joined customer IDs should match");
    }

    private WayangContext wayangContext() {
        Configuration config = new Configuration();
        config.setProperty("wayang.clickhouse.jdbc.url", JDBC_URL + "/" + DATABASE);
        config.setProperty("wayang.clickhouse.jdbc.user", USER);
        config.setProperty("wayang.clickhouse.jdbc.password", PASSWORD);
        config.getMappingProvider().addAllToWhitelist(
                Collections.singleton(new JoinFlattenMapping()));
        return new WayangContext(config)
                .withPlugin(ClickHouse.plugin());
    }

    private TableSink<Record> tableSink(String... columnNames) {
        return new TableSink<>(new Properties(), "overwrite", SINK_TABLE, columnNames);
    }

    private static MapOperator<Tuple2<Record, Record>, Record> joinFlattenOperator() {
        MapOperator<Tuple2<Record, Record>, Record> operator = new MapOperator<>(
                new TransformationDescriptor<>(
                        new JoinFlattenFunction(),
                        DataUnitType.createBasicUnchecked(Tuple2.class),
                        DataUnitType.createBasic(Record.class)),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                DataSetType.createDefault(Record.class));
        operator.setName(JOIN_FLATTEN_NAME);
        return operator;
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
                right.getField(0),
                right.getField(1),
                right.getField(2));
    }

    private long queryLong(String sql) {
        try (Connection c = jdbc(); Statement statement = c.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        } catch (Exception e) {
            throw new RuntimeException("query failed: " + sql, e);
        }
    }

    private double queryDouble(String sql) {
        try (Connection c = jdbc(); Statement statement = c.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            rs.next();
            return rs.getDouble(1);
        } catch (Exception e) {
            throw new RuntimeException("query failed: " + sql, e);
        }
    }

    private void assertSingleDoubleResult(double expected, String message) {
        try (Connection c = jdbc(); Statement statement = c.createStatement();
                ResultSet rs = statement.executeQuery("SELECT * FROM " + SINK_TABLE)) {
            assertTrue(rs.next(), message);
            assertEquals(expected, rs.getDouble(1), 0.01, message);
            assertFalse(rs.next(), message);
        } catch (Exception e) {
            throw new RuntimeException("query failed: SELECT * FROM " + SINK_TABLE, e);
        }
    }

    private Map<String, Double> readRegionSums() {
        Map<String, Double> sums = new HashMap<>();
        try (Connection c = jdbc(); Statement statement = c.createStatement();
                ResultSet rs = statement.executeQuery("SELECT * FROM " + SINK_TABLE)) {
            while (rs.next()) {
                sums.put(rs.getString(1), rs.getDouble(2));
            }
            return sums;
        } catch (Exception e) {
            throw new RuntimeException("query failed: SELECT * FROM " + SINK_TABLE, e);
        }
    }

    private static final class JoinFlattenFunction implements
            FunctionDescriptor.SerializableFunction<Tuple2<Record, Record>, Record> {

        @Override
        public Record apply(Tuple2<Record, Record> tuple) {
            return flattenJoinResult(tuple);
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
                    .withAdditionalTest(operator -> JOIN_FLATTEN_NAME.equals(((MapOperator) operator).getName()));

            ReplacementSubplanFactory factory = new ReplacementSubplanFactory.OfSingleOperators<MapOperator>(
                    (matchedOperator, epoch) -> createClickHouseProjection().at(epoch));

            return Collections.singleton(new PlanTransformation(
                    SubplanPattern.createSingleton(pattern),
                    factory,
                    ClickHousePlatform.getInstance()));
        }

        private static ClickHouseProjectionOperator createClickHouseProjection() {
            ProjectionDescriptor<Tuple2<Record, Record>, Record> descriptor = new ProjectionDescriptor<>(
                    new JoinFlattenFunction(),
                    Arrays.asList(JOIN_COLUMNS),
                    DataUnitType.createBasicUnchecked(Tuple2.class),
                    DataUnitType.createBasic(Record.class));
            MapOperator<Tuple2<Record, Record>, Record> projection = new MapOperator<>(
                    descriptor,
                    DataSetType.createDefaultUnchecked(Tuple2.class),
                    DataSetType.createDefault(Record.class));
            projection.setName(JOIN_FLATTEN_NAME);
            return new ClickHouseProjectionOperator((MapOperator<Record, Record>) (MapOperator) projection);
        }
    }

    private static Connection jdbc() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", USER);
        if (PASSWORD != null && !PASSWORD.isEmpty()) {
            p.setProperty("password", PASSWORD);
        }
        return DriverManager.getConnection(JDBC_URL, p);
    }
}
