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

package org.apache.wayang.trino;

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
import org.apache.wayang.trino.operators.TrinoProjectionOperator;
import org.apache.wayang.trino.operators.TrinoTableSource;
import org.apache.wayang.trino.platform.TrinoPlatform;
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
 * End-to-end integration tests for every operator the Trino platform implements,
 * driven through the Wayang API against a <b>live Trino</b> cluster.
 *
 * <p>Coverage: {@code TableSource}, {@code Filter}, {@code Projection},
 * {@code Join}, {@code GlobalReduce}, {@code ReduceBy}, {@code Sort},
 * and {@code TableSink}. Every Wayang plan ends in a Trino table sink so the
 * execution itself does not require the Java plugin. Result assertions use
 * plain JDBC only after the Wayang execution has completed.
 *
 * <p>Prerequisites: a user-managed Trino reachable at
 * {@code TRINO_HOST:TRINO_PORT} (defaults {@code localhost:8080}).
 * If Trino is not reachable the whole class is skipped (not failed).
 *
 * <p>Run:
 * <pre>
 *   JAVA_HOME=&lt;jdk17&gt; mvn -o test -pl wayang-platforms/wayang-trino \
 *     -Dtest=TrinoOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Drat.skip=true -Dlicense.skip=true -Pskip-prerequisite-check
 * </pre>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TrinoOperatorsIT {

    private static final String HOST = System.getenv().getOrDefault("TRINO_HOST", "localhost");
    private static final int    PORT = Integer.parseInt(System.getenv().getOrDefault("TRINO_PORT", "8080"));
    private static final String USER = System.getenv().getOrDefault("TRINO_USER", "admin");
    private static final String JDBC_URL = String.format("jdbc:trino://%s:%d", HOST, PORT);

    // Dedicated schema so the test is self-contained and side-effect free.
    private static final String SCHEMA     = "iceberg.wayang_it";
    private static final String ORDERS     = SCHEMA + ".orders";
    private static final String CUSTOMERS  = SCHEMA + ".customers";
    private static final String SINK_TABLE_NAME = "operator_result";
    private static final String SINK_TABLE = SCHEMA + "." + SINK_TABLE_NAME;
    private static final String[] JOIN_COLUMNS = {
            "order_id", "customer_id", "region", "amount", "cust_id", "name", "tier"
    };
    private static final String JOIN_FLATTEN_NAME = "Trino test-only join flatten";

    private static boolean trinoAvailable = false;

    // Lifecycle

    @BeforeAll
    static void setUp() {
        try (Connection c = jdbc()) {
            Statement st = c.createStatement();
            st.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);

            st.execute("DROP TABLE IF EXISTS " + ORDERS);
            st.execute("CREATE TABLE " + ORDERS + " ("
                    + "order_id BIGINT, customer_id BIGINT, region VARCHAR, amount DOUBLE)"
                    + " WITH (format = 'PARQUET')");
            st.execute("INSERT INTO " + ORDERS + " VALUES "
                    + "(1, 100, 'AMER', 2200.0),"
                    + "(2, 101, 'EMEA',  800.5),"
                    + "(3, 100, 'AMER',  680.5),"
                    + "(4, 102, 'APAC', 1500.0),"
                    + "(5, 101, 'EMEA', 1100.0),"
                    + "(6, 100, 'AMER',  950.25)");
            // Scale up so Wayang's cost optimizer actually elects SQL pushdown
            // (on a tiny table it prefers a full scan + Java-side ops, which
            // makes the query-history assertions fail). Trino caps sequence()
            // at 10000 entries, so scale in two steps: x10000 then x2 = 120000
            // rows total; ratios preserved (AMER = 60000).
            st.execute("INSERT INTO " + ORDERS
                    + " SELECT order_id + (n * 10), customer_id, region, amount"
                    + " FROM " + ORDERS + ", UNNEST(sequence(1, 9999)) AS t(n)");   // 6 -> 60000
            st.execute("INSERT INTO " + ORDERS
                    + " SELECT order_id + 600000, customer_id, region, amount"
                    + " FROM " + ORDERS);                                            // 60000 -> 120000

            st.execute("DROP TABLE IF EXISTS " + CUSTOMERS);
            st.execute("CREATE TABLE " + CUSTOMERS + " ("
                    + "cust_id BIGINT, name VARCHAR, tier VARCHAR)"
                    + " WITH (format = 'PARQUET')");
            st.execute("INSERT INTO " + CUSTOMERS + " VALUES "
                    + "(100, 'Acme',   'GOLD'),"
                    + "(101, 'Globex', 'SILVER'),"
                    + "(102, 'Initech','BRONZE')");

            trinoAvailable = true;
            System.out.println("[TrinoOperatorsIT] Connected to Trino at " + JDBC_URL + " — fixtures created.");
        } catch (Exception e) {
            System.err.println("[TrinoOperatorsIT] Trino not available (" + e.getMessage() + ") — skipping.");
        }
    }

    @AfterAll
    static void tearDown() {
        if (!trinoAvailable) return;
        try (Connection c = jdbc()) {
            Statement st = c.createStatement();
            st.execute("DROP TABLE IF EXISTS " + ORDERS);
            st.execute("DROP TABLE IF EXISTS " + CUSTOMERS);
            st.execute("DROP TABLE IF EXISTS " + SINK_TABLE);
        } catch (Exception e) {
            System.err.println("[TrinoOperatorsIT] cleanup failed: " + e.getMessage());
        }
    }

    // Tests (one per operator)

    /** TableSource: full scan returns every row. */
    @Test
    @Order(1)
    void tableSource() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");
        TrinoTableSource src = new TrinoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
        TableSink<Record> sink = tableSink("order_id", "customer_id", "region", "amount");
        src.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(120000, queryLong("SELECT count(*) FROM " + SINK_TABLE),
                "TableSource should return all orders");
        assertSqlReachedTrino("SELECT * FROM " + ORDERS);
    }

    /** Filter: WHERE region = 'AMER' pushed to Trino. */
    @Test
    @Order(2)
    void filter() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");
        TrinoTableSource src = new TrinoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
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
        assertEquals(0, queryLong("SELECT count_if(region <> 'AMER') FROM " + SINK_TABLE), "all rows AMER");
        assertSqlReachedTrino("WHERE region = 'AMER'");
    }

    /** Projection (+filter): only the projected columns are fetched. */
    @Test
    @Order(3)
    void projection() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");
        TrinoTableSource src = new TrinoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
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
        assertEquals(2, queryLong(
                "SELECT count(*) FROM iceberg.information_schema.columns "
                        + "WHERE table_schema = 'wayang_it' AND table_name = '" + SINK_TABLE_NAME + "'"),
                "projection keeps only 2 columns");
        assertSqlReachedTrino("SELECT region, amount FROM " + ORDERS);
    }

    /**
     * Join: orders and customers on customer_id.
     *
     * <p>The logical join emits {@code Tuple2<Record,Record>}. A test-only
     * mapping turns the following flatten map into a Trino SQL projection, so
     * this plan still executes entirely in Trino without deciding the general
     * Tuple-to-Record semantics for JDBC platforms.
     */
    @Test
    @Order(4)
    void join() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");
        TrinoTableSource orders = new TrinoTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount");
        TrinoTableSource customers = new TrinoTableSource(
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
        assertSqlReachedTrino("JOIN " + CUSTOMERS);
    }

    /** GlobalReduce: SUM(amount) over the whole table collapses to a single row. */
    @Test
    @Order(5)
    void globalReduce() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");
        TrinoTableSource src = new TrinoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
        GlobalReduceOperator<Record> reduce = new GlobalReduceOperator<>(
                new ReduceDescriptor<>((a, b) -> a, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount"),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = tableSink("total_amount");
        src.connectTo(0, reduce, 0);
        reduce.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        // 6 base rows sum to 7231.25; scaled x20000 gives 144,625,000 (exact in doubles).
        assertSingleDoubleResult(144_625_000.0, "global reduce must collapse to a single row");
        assertSqlReachedTrino("SELECT SUM(amount) AS total_amount FROM " + ORDERS);
    }

    /** ReduceBy: SUM(amount) GROUP BY region yields one row per region. */
    @Test
    @Order(6)
    void reduceBy() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");
        TrinoTableSource src = new TrinoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
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
        // Base sums (AMER 3830.75, EMEA 1900.5, APAC 1500.0) scaled x20000.
        assertEquals(76_615_000.0, sums.get("AMER"), 0.01);
        assertEquals(38_010_000.0, sums.get("EMEA"), 0.01);
        assertEquals(30_000_000.0, sums.get("APAC"), 0.01);
        assertSqlReachedTrino("GROUP BY region");
    }

    /** Sort: ORDER BY amount ASC pushed to Trino, order preserved to the sink. */
    @Test
    @Order(7)
    void sort() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");
        TrinoTableSource src = new TrinoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
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
        assertSqlReachedTrino("ORDER BY amount ASC");
    }

    /**
     * TableSink: filter + sink composed into a single {@code CREATE TABLE ... AS
     * SELECT} that runs entirely inside Trino; no data leaves the database.
     */
    @Test
    @Order(8)
    void tableSink() throws Exception {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");

        TrinoTableSource src = new TrinoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
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
        assertSqlReachedTrino("CREATE TABLE " + SINK_TABLE + " AS");
    }

    // JavaPlanBuilder combination tests

    /**
     * JavaPlanBuilder API: read a table, filter it, project two columns, and
     * write the result through a complete high-level Wayang plan.
     */
    @Test
    @Order(9)
    void javaPlanBuilderReadTableFilterProjection() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");

        new JavaPlanBuilder(
                wayangContext(), "Trino JavaPlanBuilder readTable integration test")
                .readTable(new TrinoTableSource(
                        ORDERS, "order_id", "customer_id", "region", "amount"))
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
        assertSqlReachedTrino(
                "SELECT order_id, amount FROM " + ORDERS + " WHERE region = 'AMER'");
    }

    /**
     * JavaPlanBuilder API: combine a filter with a global reduction.
     */
    @Test
    @Order(10)
    void javaPlanBuilderReadTableFilterGlobalReduce() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");

        new JavaPlanBuilder(
                wayangContext(), "Trino JavaPlanBuilder global reduce integration test")
                .readTable(new TrinoTableSource(
                        ORDERS, "order_id", "customer_id", "region", "amount"))
                .filter(record -> "AMER".equals(record.getField(2)))
                    .withSqlUdf("region = 'AMER'")
                .reduce((left, right) -> left)
                    .withSqlUdf("SUM(amount) AS total_amount")
                .writeTable(SINK_TABLE, "overwrite", new String[]{"total_amount"}, new Properties());

        assertSingleDoubleResult(76_615_000.0, "global reduction should return one row");
        assertSqlReachedTrino(
                "SELECT SUM(amount) AS total_amount FROM " + ORDERS + " WHERE region = 'AMER'");
    }

    /**
     * JavaPlanBuilder API: group by region, aggregate each group, and sort the
     * grouped result.
     */
    @Test
    @Order(11)
    void javaPlanBuilderReadTableReduceBySort() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");

        new JavaPlanBuilder(
                wayangContext(), "Trino JavaPlanBuilder reduce-by and sort integration test")
                .readTable(new TrinoTableSource(
                        ORDERS, "order_id", "customer_id", "region", "amount"))
                .reduceByKey(
                        record -> new Record(record.getField(2)),
                        (left, right) -> left)
                    .withSqlUdfs("region", "SUM(amount) AS total_amount")
                .sort(record -> new Record(record.getField(0)))
                    .withSqlUdf("region", "ASC")
                .writeTable(SINK_TABLE, "overwrite", new String[]{"region", "total_amount"}, new Properties());

        assertEquals("AMER,APAC,EMEA", queryString(
                "SELECT array_join(array_agg(region ORDER BY region), ',') FROM " + SINK_TABLE),
                "one row per region expected");
        assertSqlReachedTrino(
                "SELECT region,SUM(amount) AS total_amount FROM " + ORDERS
                        + " GROUP BY region ORDER BY region ASC");
    }

    /**
     * JavaPlanBuilder API: compose a filtered projection directly into a table
     * sink so all processing remains in Trino.
     */
    @Test
    @Order(12)
    void javaPlanBuilderReadTableFilterProjectionTableSink() throws Exception {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");

        new JavaPlanBuilder(wayangContext(), "Trino JavaPlanBuilder table sink integration test")
                .readTable(new TrinoTableSource(
                        ORDERS, "order_id", "customer_id", "region", "amount"))
                .filter(record -> "AMER".equals(record.getField(2)))
                    .withSqlUdf("region = 'AMER'")
                .asRecords()
                .projectRecords(new String[]{"order_id", "amount"})
                .writeTable(
                        SINK_TABLE,
                        "overwrite",
                        new String[]{"order_id", "amount"},
                        new Properties());

        try (Connection c = jdbc()) {
            ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM " + SINK_TABLE);
            rs.next();
            assertEquals(60000, rs.getLong(1), "sink table should contain projected AMER orders");
        }
        assertSqlReachedTrino(
                "CREATE TABLE " + SINK_TABLE
                        + " AS SELECT order_id, amount FROM " + ORDERS + " WHERE region = 'AMER'");
    }

    /**
     * JavaPlanBuilder API: join two tables, flatten through the test-only Trino
     * mapping, and write the result in Trino.
     */
    @Test
    @Order(13)
    void javaPlanBuilderReadTableJoin() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");

        JavaPlanBuilder plan = new JavaPlanBuilder(
                wayangContext(), "Trino JavaPlanBuilder join integration test");
        DataQuantaBuilder<?, Record> orders = plan.readTable(new TrinoTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount"));
        DataQuantaBuilder<?, Record> customers = plan.readTable(new TrinoTableSource(
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
        assertSqlReachedTrino("JOIN " + CUSTOMERS);
    }

    private WayangContext wayangContext() {
        Configuration config = new Configuration();
        config.setProperty("wayang.trino.jdbc.url", JDBC_URL);
        config.setProperty("wayang.trino.jdbc.user", USER);
        config.setProperty("wayang.trino.jdbc.password", "");
        config.getMappingProvider().addAllToWhitelist(
                Collections.singleton(new JoinFlattenMapping()));
        return new WayangContext(config)
                .withPlugin(Trino.plugin());
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

    private String queryString(String sql) {
        try (Connection c = jdbc(); Statement statement = c.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
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
                    (matchedOperator, epoch) -> createTrinoProjection().at(epoch));

            return Collections.singleton(new PlanTransformation(
                    SubplanPattern.createSingleton(pattern),
                    factory,
                    TrinoPlatform.getInstance()));
        }

        private static TrinoProjectionOperator createTrinoProjection() {
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
            return new TrinoProjectionOperator((MapOperator<Record, Record>) (MapOperator) projection);
        }
    }

    /** Assert that Trino actually ran a query containing the given fragment. */
    private void assertSqlReachedTrino(String fragment) {
        try (Connection c = jdbc()) {
            ResultSet rs = c.createStatement().executeQuery(
                    "SELECT count(*) FROM system.runtime.queries "
                  + "WHERE query LIKE '%" + fragment.replace("'", "''") + "%' "
                  + "AND query NOT LIKE '%system.runtime%'");
            rs.next();
            assertTrue(rs.getLong(1) > 0,
                    "Expected a Trino query containing: " + fragment);
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
