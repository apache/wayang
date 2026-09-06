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

package org.apache.wayang.presto;

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
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.presto.operators.PrestoProjectionOperator;
import org.apache.wayang.presto.operators.PrestoTableSource;
import org.apache.wayang.presto.platform.PrestoPlatform;
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
 * End-to-end integration tests for every operator the Presto platform implements,
 * driven through the Wayang API against a <b>live PrestoDB</b> cluster (in-memory
 * connector).
 *
 * <p>Coverage: {@code TableSource}, {@code Filter}, {@code Projection},
 * {@code Join}, {@code GlobalReduce}, {@code ReduceBy}, {@code Sort},
 * and {@code TableSink}, plus the same operators driven through the high-level
 * {@link JavaPlanBuilder} API. Every Wayang plan ends in a Presto table sink so
 * the execution itself does not require the Java plugin — only {@code Presto.plugin()}
 * is registered. Result assertions use plain JDBC only after the Wayang execution
 * has completed; the sink table's existence and contents prove that the composed
 * {@code CREATE TABLE ... AS SELECT} ran entirely inside Presto.
 *
 * <p>Prerequisites: a Presto reachable at {@code PRESTO_HOST:PRESTO_PORT}
 * (defaults {@code localhost:8080}) with the {@code memory} connector enabled.
 * If Presto is not reachable
 * the whole class is skipped (not failed).
 *
 * <pre>
 *   JAVA_HOME=&lt;jdk17&gt; mvn -o test -pl wayang-platforms/wayang-presto \
 *     -Dtest=PrestoOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Drat.skip=true -Dlicense.skip=true -Pskip-prerequisite-check
 * </pre>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PrestoOperatorsIT {

    private static final String HOST = System.getenv().getOrDefault("PRESTO_HOST", "localhost");
    private static final int    PORT = Integer.parseInt(System.getenv().getOrDefault("PRESTO_PORT", "8080"));
    private static final String USER = System.getenv().getOrDefault("PRESTO_USER", "test");
    private static final String JDBC_URL = String.format("jdbc:presto://%s:%d/memory", HOST, PORT);

    private static final String SCHEMA          = "memory.wayang_it";
    private static final String ORDERS          = SCHEMA + ".orders";
    private static final String CUSTOMERS       = SCHEMA + ".customers";
    private static final String SINK_TABLE_NAME = "operator_result";
    private static final String SINK_TABLE      = SCHEMA + "." + SINK_TABLE_NAME;
    private static final String[] JOIN_COLUMNS = {
            "order_id", "customer_id", "region", "amount", "cust_id", "name", "tier"
    };
    private static final String JOIN_FLATTEN_NAME = "Presto test-only join flatten";

    private static boolean prestoAvailable = false;

    // Lifecycle

    @BeforeAll
    static void setUp() throws Exception {
        try (Connection probe = jdbc()) {
            probe.createStatement().execute("SELECT 1");
            prestoAvailable = true;
        } catch (Exception e) {
            System.err.println("[PrestoOperatorsIT] Presto not reachable (" + e.getMessage() + ") — skipping.");
            return;
        }
        try (Connection c = jdbc()) {
            Statement st = c.createStatement();
            st.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);

            st.execute("DROP TABLE IF EXISTS " + ORDERS);
            st.execute("CREATE TABLE " + ORDERS
                    + " (order_id BIGINT, customer_id BIGINT, region VARCHAR, amount DOUBLE)");
            // 120000 rows from a 6-row VALUES list crossed with sequence(1,10000) and a
            // 2-row doubler. Sourcing from VALUES (not the table itself) avoids reading +
            // writing the same memory table in one statement. AMER = 60000.
            st.execute("INSERT INTO " + ORDERS + " (order_id, customer_id, region, amount) "
                    + "SELECT CAST(b.order_id AS BIGINT), CAST(b.customer_id AS BIGINT), "
                    + "       b.region, CAST(b.amount AS DOUBLE) "
                    + "FROM UNNEST(sequence(1, 10000)) AS s(n) "
                    + "CROSS JOIN (VALUES (1), (2)) AS d(k) "
                    + "CROSS JOIN (VALUES "
                    + "  (1, 100, 'AMER', 2200.0),"
                    + "  (2, 101, 'EMEA',  800.5),"
                    + "  (3, 100, 'AMER',  680.5),"
                    + "  (4, 102, 'APAC', 1500.0),"
                    + "  (5, 101, 'EMEA', 1100.0),"
                    + "  (6, 100, 'AMER',  950.25)"
                    + ") AS b(order_id, customer_id, region, amount)");

            // customers' key column is `cust_id` (not customer_id) so the flattened
            // join projection has no duplicate column name in CREATE TABLE AS SELECT.
            st.execute("DROP TABLE IF EXISTS " + CUSTOMERS);
            st.execute("CREATE TABLE " + CUSTOMERS
                    + " (cust_id BIGINT, name VARCHAR, tier VARCHAR)");
            st.execute("INSERT INTO " + CUSTOMERS + " VALUES "
                    + "(CAST(100 AS BIGINT), 'Acme',    'GOLD'),"
                    + "(101, 'Globex',  'SILVER'),"
                    + "(102, 'Initech', 'BRONZE')");
        }
        System.out.println("[PrestoOperatorsIT] Connected to Presto at " + JDBC_URL + " — fixtures created.");
    }

    @AfterAll
    static void tearDown() {
        if (!prestoAvailable) return;
        try (Connection c = jdbc()) {
            Statement st = c.createStatement();
            st.execute("DROP TABLE IF EXISTS " + ORDERS);
            st.execute("DROP TABLE IF EXISTS " + CUSTOMERS);
            st.execute("DROP TABLE IF EXISTS " + SINK_TABLE);
        } catch (Exception e) {
            System.err.println("[PrestoOperatorsIT] cleanup failed: " + e.getMessage());
        }
    }

    // Tests (one per operator)

    /** TableSource: full scan returns every row. */
    @Test
    @Order(1)
    void tableSource() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");
        PrestoTableSource src = new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
        TableSink<Record> sink = tableSink("order_id", "customer_id", "region", "amount");
        src.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(120000, queryLong("SELECT count(*) FROM " + SINK_TABLE),
                "TableSource should return all orders");
        assertSqlReachedPresto("SELECT * FROM " + ORDERS);
    }

    /** Filter: WHERE region = 'AMER' pushed to Presto. */
    @Test
    @Order(2)
    void filter() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");
        PrestoTableSource src = new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record r) -> "AMER".equals(r.getField(2)), Record.class
                ).withSqlImplementation("region = 'AMER'"));
        TableSink<Record> sink = tableSink("order_id", "customer_id", "region", "amount");
        src.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(60000, queryLong("SELECT count(*) FROM " + SINK_TABLE), "60000 AMER orders expected");
        assertEquals(0, queryLong("SELECT count_if(region <> 'AMER') FROM " + SINK_TABLE), "all rows AMER");
        assertSqlReachedPresto("WHERE region = 'AMER'");
    }

    /** Projection (+filter): only the projected columns are fetched. */
    @Test
    @Order(3)
    void projection() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");
        PrestoTableSource src = new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
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

        assertEquals(60000, queryLong("SELECT count(*) FROM " + SINK_TABLE), "60000 AMER rows expected");
        assertEquals(2, columnCount(SINK_TABLE), "projection keeps only 2 columns");
        assertSqlReachedPresto("SELECT region, amount FROM " + ORDERS);
    }

    /**
     * Join: orders and customers on customer_id, flattened into Presto SQL via a
     * test-only mapping so the whole plan stays in Presto.
     */
    @Test
    @Order(4)
    void join() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");
        PrestoTableSource orders = new PrestoTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount");
        PrestoTableSource customers = new PrestoTableSource(
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
        assertEquals(0, queryLong("SELECT count_if(customer_id <> cust_id) FROM " + SINK_TABLE),
                "joined customer IDs should match");
        assertSqlReachedPresto("JOIN " + CUSTOMERS);
    }

    /** GlobalReduce: SUM(amount) over the whole table collapses to a single row. */
    @Test
    @Order(5)
    void globalReduce() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");
        PrestoTableSource src = new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
        GlobalReduceOperator<Record> reduce = new GlobalReduceOperator<>(
                new ReduceDescriptor<>((a, b) -> a, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount"),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = tableSink("total_amount");
        src.connectTo(0, reduce, 0);
        reduce.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertSingleDoubleResult(144_625_000.0, "global reduce must collapse to a single row");
        assertSqlReachedPresto("SELECT SUM(amount) AS total_amount FROM " + ORDERS);
    }

    /** ReduceBy: SUM(amount) GROUP BY region yields one row per region. */
    @Test
    @Order(6)
    void reduceBy() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");
        PrestoTableSource src = new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
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
        assertSqlReachedPresto("GROUP BY region");
    }

    /** Sort: ORDER BY amount ASC pushed to Presto. */
    @Test
    @Order(7)
    void sort() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");
        PrestoTableSource src = new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
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
        assertSqlReachedPresto("ORDER BY amount ASC");
    }

    /** TableSink: filter + sink composed into a single CREATE TABLE AS SELECT in Presto. */
    @Test
    @Order(8)
    void tableSink() throws Exception {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");

        PrestoTableSource src = new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
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
                    "SELECT count(*), count_if(region <> 'AMER') FROM " + SINK_TABLE);
            rs.next();
            assertEquals(60000, rs.getLong(1), "sink table must hold all AMER orders");
            assertEquals(0, rs.getLong(2), "sink table must hold only AMER orders");
        }
        assertSqlReachedPresto("CREATE TABLE " + SINK_TABLE + " AS");
    }

    // JavaPlanBuilder combination tests

    /** JavaPlanBuilder: read, filter, project, write — all in Presto. */
    @Test
    @Order(9)
    void javaPlanBuilderReadTableFilterProjection() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");

        new JavaPlanBuilder(wayangContext(), "Presto JavaPlanBuilder readTable integration test")
                .readTable(new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount"))
                .filter(record -> "AMER".equals(record.getField(2)))
                    .withSqlUdf("region = 'AMER'")
                .asRecords()
                .projectRecords(new String[]{"order_id", "amount"})
                .writeTable(SINK_TABLE, "overwrite", new String[]{"order_id", "amount"}, new Properties());

        assertEquals(60000, queryLong("SELECT count(*) FROM " + SINK_TABLE),
                "60000 projected AMER orders expected");
        assertEquals(0, queryLong(
                "SELECT count_if(amount NOT IN (2200.0, 680.5, 950.25)) FROM " + SINK_TABLE),
                "only AMER order amounts should remain");
        assertSqlReachedPresto("SELECT order_id, amount FROM " + ORDERS + " WHERE region = 'AMER'");
    }

    /** JavaPlanBuilder: filter + global reduce, written in Presto. */
    @Test
    @Order(10)
    void javaPlanBuilderReadTableFilterGlobalReduce() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");

        new JavaPlanBuilder(wayangContext(), "Presto JavaPlanBuilder global reduce integration test")
                .readTable(new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount"))
                .filter(record -> "AMER".equals(record.getField(2)))
                    .withSqlUdf("region = 'AMER'")
                .reduce((left, right) -> left)
                    .withSqlUdf("SUM(amount) AS total_amount")
                .writeTable(SINK_TABLE, "overwrite", new String[]{"total_amount"}, new Properties());

        assertSingleDoubleResult(76_615_000.0, "global reduction should return one row");
        assertSqlReachedPresto("SELECT SUM(amount) AS total_amount FROM " + ORDERS + " WHERE region = 'AMER'");
    }

    /** JavaPlanBuilder: reduce-by + sort, written in Presto. */
    @Test
    @Order(11)
    void javaPlanBuilderReadTableReduceBySort() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");

        new JavaPlanBuilder(wayangContext(), "Presto JavaPlanBuilder reduce-by and sort integration test")
                .readTable(new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount"))
                .reduceByKey(
                        record -> new Record(record.getField(2)),
                        (left, right) -> left)
                    .withSqlUdfs("region", "SUM(amount) AS total_amount")
                .sort(record -> new Record(record.getField(0)))
                    .withSqlUdf("region", "ASC")
                .writeTable(SINK_TABLE, "overwrite", new String[]{"region", "total_amount"}, new Properties());

        Map<String, Double> sums = readRegionSums();
        assertEquals(3, sums.size(), "one row per region expected");
        assertTrue(sums.containsKey("AMER") && sums.containsKey("APAC") && sums.containsKey("EMEA"));
        assertSqlReachedPresto(
                "SELECT region,SUM(amount) AS total_amount FROM " + ORDERS
                        + " GROUP BY region ORDER BY region ASC");
    }

    /** JavaPlanBuilder: filtered projection straight into a Presto table sink. */
    @Test
    @Order(12)
    void javaPlanBuilderReadTableFilterProjectionTableSink() throws Exception {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");

        new JavaPlanBuilder(wayangContext(), "Presto JavaPlanBuilder table sink integration test")
                .readTable(new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount"))
                .filter(record -> "AMER".equals(record.getField(2)))
                    .withSqlUdf("region = 'AMER'")
                .asRecords()
                .projectRecords(new String[]{"order_id", "amount"})
                .writeTable(SINK_TABLE, "overwrite", new String[]{"order_id", "amount"}, new Properties());

        assertEquals(60000, queryLong("SELECT count(*) FROM " + SINK_TABLE),
                "sink table should contain projected AMER orders");
        assertSqlReachedPresto(
                "CREATE TABLE " + SINK_TABLE + " AS SELECT order_id, amount FROM " + ORDERS
                        + " WHERE region = 'AMER'");
    }

    /** JavaPlanBuilder: join two tables, flatten, write — all in Presto. */
    @Test
    @Order(13)
    void javaPlanBuilderReadTableJoin() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");

        JavaPlanBuilder plan = new JavaPlanBuilder(
                wayangContext(), "Presto JavaPlanBuilder join integration test");
        DataQuantaBuilder<?, Record> orders = plan.readTable(new PrestoTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount"));
        DataQuantaBuilder<?, Record> customers = plan.readTable(new PrestoTableSource(
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
        assertEquals(0, queryLong("SELECT count_if(customer_id <> cust_id) FROM " + SINK_TABLE),
                "joined customer IDs should match");
        assertSqlReachedPresto("JOIN " + CUSTOMERS);
    }

    // Helpers

    private WayangContext wayangContext() {
        Configuration config = new Configuration();
        config.setProperty("wayang.presto.jdbc.url", JDBC_URL);
        config.setProperty("wayang.presto.jdbc.user", USER);
        // No password: the Presto JDBC driver rejects an (even empty) password on a
        // non-SSL connection.
        config.getMappingProvider().addAllToWhitelist(
                Collections.singleton(new JoinFlattenMapping()));
        return new WayangContext(config)
                .withPlugin(Presto.plugin());
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
                left.getField(0), left.getField(1), left.getField(2), left.getField(3),
                right.getField(0), right.getField(1), right.getField(2));
    }

    private long queryLong(String sql) {
        try (Connection c = jdbc(); Statement statement = c.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        } catch (Exception e) {
            throw new RuntimeException("query failed: " + sql, e);
        }
    }

    private double queryDouble(String sql) {
        try (Connection c = jdbc(); Statement statement = c.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            rs.next();
            return rs.getDouble(1);
        } catch (Exception e) {
            throw new RuntimeException("query failed: " + sql, e);
        }
    }

    private int columnCount(String table) {
        try (Connection c = jdbc(); Statement statement = c.createStatement();
                ResultSet rs = statement.executeQuery("SELECT * FROM " + table + " LIMIT 1")) {
            return rs.getMetaData().getColumnCount();
        } catch (Exception e) {
            throw new RuntimeException("query failed: column count of " + table, e);
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

    /** Test-only mapping for the unresolved logical join Tuple-to-Record mismatch. */
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
                    (matchedOperator, epoch) -> createPrestoProjection().at(epoch));

            return Collections.singleton(new PlanTransformation(
                    SubplanPattern.createSingleton(pattern),
                    factory,
                    PrestoPlatform.getInstance()));
        }

        private static PrestoProjectionOperator createPrestoProjection() {
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
            return new PrestoProjectionOperator((MapOperator<Record, Record>) (MapOperator) projection);
        }
    }

    /** Assert that Presto actually ran a query containing the given fragment. */
    private void assertSqlReachedPresto(String fragment) {
        try (Connection c = jdbc()) {
            ResultSet rs = c.createStatement().executeQuery(
                    "SELECT count(*) FROM system.runtime.queries "
                  + "WHERE state = 'FINISHED' "
                  + "AND query LIKE '%" + fragment.replace("'", "''") + "%' "
                  + "AND query NOT LIKE '%system.runtime%'");
            rs.next();
            assertTrue(rs.getLong(1) > 0, "Expected a Presto query containing: " + fragment);
        } catch (Exception e) {
            throw new RuntimeException("query-history check failed", e);
        }
    }

    private static Connection jdbc() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", USER);
        return DriverManager.getConnection(JDBC_URL, p);
    }
}
