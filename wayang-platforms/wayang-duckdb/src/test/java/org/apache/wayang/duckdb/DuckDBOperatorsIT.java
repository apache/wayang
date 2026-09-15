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

package org.apache.wayang.duckdb;

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
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.duckdb.operators.DuckDBProjectionOperator;
import org.apache.wayang.duckdb.operators.DuckDBTableSource;
import org.apache.wayang.duckdb.platform.DuckDBPlatform;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * Embedded end-to-end tests for every operator the DuckDB platform implements.
 *
 * <p>Coverage mirrors {@code TrinoOperatorsIT}: {@code TableSource},
 * {@code Filter}, {@code Projection}, {@code Join}, {@code GlobalReduce},
 * {@code ReduceBy}, {@code Sort}, and {@code TableSink}, plus five
 * JavaPlanBuilder combinations. Each Wayang plan registers only
 * {@link DuckDB#plugin()} and ends in a DuckDB table sink, so no Java-side
 * operator implementation is needed to compute the result.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DuckDBOperatorsIT {

    private static final String SCHEMA = "wayang_it";
    private static final String ORDERS = SCHEMA + ".orders";
    private static final String CUSTOMERS = SCHEMA + ".customers";
    private static final String SINK_TABLE_NAME = "operator_result";
    private static final String SINK_TABLE = SCHEMA + "." + SINK_TABLE_NAME;
    private static final String[] JOIN_COLUMNS = {
            "order_id", "customer_id", "region", "amount", "cust_id", "name", "tier"
    };
    private static final String JOIN_FLATTEN_NAME = "DuckDB test-only join flatten";

    private static Path databaseFile;
    private static String jdbcUrl;
    private static boolean deleteDatabaseFile;

    @BeforeAll
    static void setUp() throws Exception {
        jdbcUrl = System.getProperty(
                "duckdb.url",
                System.getenv().getOrDefault("DUCKDB_JDBC_URL", ""));
        if (jdbcUrl.isEmpty()) {
            databaseFile = Files.createTempFile("wayang-duckdb-operators-", ".duckdb");
            Files.deleteIfExists(databaseFile);
            jdbcUrl = "jdbc:duckdb:" + databaseFile.toAbsolutePath();
            deleteDatabaseFile = true;
        } else {
            jdbcUrl = normalizeDuckDbUrl(jdbcUrl);
        }

        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
            statement.execute("DROP TABLE IF EXISTS " + SINK_TABLE);
            statement.execute("DROP TABLE IF EXISTS " + ORDERS);
            statement.execute("DROP TABLE IF EXISTS " + CUSTOMERS);
            statement.execute("CREATE TABLE " + ORDERS + " ("
                    + "order_id BIGINT, customer_id BIGINT, region VARCHAR, amount DOUBLE)");
            statement.execute("INSERT INTO " + ORDERS + " VALUES "
                    + "(1, 100, 'AMER', 2200.0),"
                    + "(2, 101, 'EMEA',  800.5),"
                    + "(3, 100, 'AMER',  680.5),"
                    + "(4, 102, 'APAC', 1500.0),"
                    + "(5, 101, 'EMEA', 1100.0),"
                    + "(6, 100, 'AMER',  950.25)");

            statement.execute("CREATE TABLE " + CUSTOMERS + " ("
                    + "cust_id BIGINT, name VARCHAR, tier VARCHAR)");
            statement.execute("INSERT INTO " + CUSTOMERS + " VALUES "
                    + "(100, 'Acme',   'GOLD'),"
                    + "(101, 'Globex', 'SILVER'),"
                    + "(102, 'Initech','BRONZE')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (deleteDatabaseFile && databaseFile != null) {
            Files.deleteIfExists(databaseFile);
            Files.deleteIfExists(databaseFile.resolveSibling(databaseFile.getFileName() + ".wal"));
        }
    }

    @Test
    @Order(1)
    void loadsDuckDbDriverAndRunsQuery() throws Exception {
        Class.forName(DuckDBPlatform.getInstance().getJdbcDriverClassName());

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1")) {
            resultSet.next();
            assertEquals(1, resultSet.getInt(1));
        }
    }

    @Test
    @Order(2)
    void tableSource() {
        DuckDBTableSource src = new DuckDBTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
        TableSink<Record> sink = tableSink("order_id", "customer_id", "region", "amount");
        src.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(6, queryLong("SELECT count(*) FROM " + SINK_TABLE));
    }

    @Test
    @Order(3)
    void filter() {
        DuckDBTableSource src = new DuckDBTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record record) -> "AMER".equals(record.getField(2)), Record.class)
                        .withSqlImplementation("region = 'AMER'"));
        TableSink<Record> sink = tableSink("order_id", "customer_id", "region", "amount");
        src.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(3, queryLong("SELECT count(*) FROM " + SINK_TABLE));
        assertEquals(0, queryLong("SELECT SUM(CASE WHEN region <> 'AMER' THEN 1 ELSE 0 END) FROM " + SINK_TABLE));
    }

    @Test
    @Order(4)
    void projection() {
        DuckDBTableSource src = new DuckDBTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
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
        TableSink<Record> sink = tableSink("region", "amount");
        src.connectTo(0, filter, 0);
        filter.connectTo(0, projection, 0);
        projection.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(3, queryLong("SELECT count(*) FROM " + SINK_TABLE));
        assertEquals(2, queryLong(
                "SELECT count(*) FROM information_schema.columns "
                        + "WHERE table_schema = '" + SCHEMA + "' AND table_name = '" + SINK_TABLE_NAME + "'"));
    }

    @Test
    @Order(5)
    void join() {
        DuckDBTableSource orders = new DuckDBTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount");
        DuckDBTableSource customers = new DuckDBTableSource(
                CUSTOMERS, "cust_id", "name", "tier");
        JoinOperator<Record, Record, Record> join = new JoinOperator<>(
                new TransformationDescriptor<>(
                        (Record record) -> new Record(record.getField(1)), Record.class, Record.class)
                        .withSqlImplementation(ORDERS, "customer_id"),
                new TransformationDescriptor<>(
                        (Record record) -> new Record(record.getField(0)), Record.class, Record.class)
                        .withSqlImplementation(CUSTOMERS, "cust_id"));
        MapOperator<Tuple2<Record, Record>, Record> flatten = joinFlattenOperator();
        TableSink<Record> sink = tableSink(JOIN_COLUMNS);
        orders.connectTo(0, join, 0);
        customers.connectTo(0, join, 1);
        join.connectTo(0, flatten, 0);
        flatten.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(6, queryLong("SELECT count(*) FROM " + SINK_TABLE));
        assertEquals(0, queryLong("SELECT SUM(CASE WHEN customer_id <> cust_id THEN 1 ELSE 0 END) FROM "
                + SINK_TABLE));
    }

    @Test
    @Order(6)
    void globalReduce() {
        DuckDBTableSource src = new DuckDBTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
        GlobalReduceOperator<Record> reduce = new GlobalReduceOperator<>(
                new ReduceDescriptor<>((left, right) -> left, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount"),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = tableSink("total_amount");
        src.connectTo(0, reduce, 0);
        reduce.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertSingleDoubleResult(7231.25);
    }

    @Test
    @Order(7)
    void reduceBy() {
        DuckDBTableSource src = new DuckDBTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
        ReduceByOperator<Record, Record> reduceBy = new ReduceByOperator<>(
                new TransformationDescriptor<>(
                        (Record record) -> new Record(record.getField(2)), Record.class, Record.class)
                        .withSqlImplementation("region", "region"),
                new ReduceDescriptor<>((left, right) -> left, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount"),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = tableSink("region", "total_amount");
        src.connectTo(0, reduceBy, 0);
        reduceBy.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        Map<String, Double> sums = readRegionSums();
        assertEquals(3, sums.size());
        assertEquals(3830.75, sums.get("AMER"), 0.01);
        assertEquals(1900.5, sums.get("EMEA"), 0.01);
        assertEquals(1500.0, sums.get("APAC"), 0.01);
    }

    @Test
    @Order(8)
    void sort() {
        DuckDBTableSource src = new DuckDBTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
        SortOperator<Record, Record> sort = new SortOperator<>(
                new TransformationDescriptor<>(
                        (Record record) -> new Record(record.getField(3)), Record.class, Record.class)
                        .withSqlImplementation("amount", "ASC"),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = tableSink("order_id", "customer_id", "region", "amount");
        src.connectTo(0, sort, 0);
        sort.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(6, queryLong("SELECT count(*) FROM " + SINK_TABLE));
        assertEquals(680.5, queryDouble("SELECT min(amount) FROM " + SINK_TABLE), 0.001);
        assertEquals(2200.0, queryDouble("SELECT max(amount) FROM " + SINK_TABLE), 0.001);
    }

    @Test
    @Order(9)
    void tableSink() {
        DuckDBTableSource src = new DuckDBTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record record) -> "AMER".equals(record.getField(2)), Record.class)
                        .withSqlImplementation("region = 'AMER'"));
        TableSink<Record> sink = new TableSink<>(
                new Properties(), "overwrite", SINK_TABLE,
                "order_id", "customer_id", "region", "amount");
        src.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        assertEquals(3, queryLong("SELECT count(*) FROM " + SINK_TABLE));
        assertEquals(0, queryLong("SELECT SUM(CASE WHEN region <> 'AMER' THEN 1 ELSE 0 END) FROM " + SINK_TABLE));
    }

    @Test
    @Order(10)
    void javaPlanBuilderReadTableFilterProjection() {
        new JavaPlanBuilder(
                wayangContext(), "DuckDB JavaPlanBuilder readTable integration test")
                .readTable(new DuckDBTableSource(
                        ORDERS, "order_id", "customer_id", "region", "amount"))
                .filter(record -> "AMER".equals(record.getField(2)))
                    .withSqlUdf("region = 'AMER'")
                .asRecords()
                .projectRecords(new String[]{"order_id", "amount"})
                .writeTable(SINK_TABLE, "overwrite", new String[]{"order_id", "amount"}, new Properties());

        assertEquals(3, queryLong("SELECT count(*) FROM " + SINK_TABLE));
        assertEquals(0, queryLong(
                "SELECT SUM(CASE WHEN amount NOT IN (2200.0, 680.5, 950.25) THEN 1 ELSE 0 END) FROM "
                        + SINK_TABLE));
    }

    @Test
    @Order(11)
    void javaPlanBuilderReadTableFilterGlobalReduce() {
        new JavaPlanBuilder(
                wayangContext(), "DuckDB JavaPlanBuilder global reduce integration test")
                .readTable(new DuckDBTableSource(
                        ORDERS, "order_id", "customer_id", "region", "amount"))
                .filter(record -> "AMER".equals(record.getField(2)))
                    .withSqlUdf("region = 'AMER'")
                .reduce((left, right) -> left)
                    .withSqlUdf("SUM(amount) AS total_amount")
                .writeTable(SINK_TABLE, "overwrite", new String[]{"total_amount"}, new Properties());

        assertSingleDoubleResult(3830.75);
    }

    @Test
    @Order(12)
    void javaPlanBuilderReadTableReduceBySort() {
        new JavaPlanBuilder(
                wayangContext(), "DuckDB JavaPlanBuilder reduce-by and sort integration test")
                .readTable(new DuckDBTableSource(
                        ORDERS, "order_id", "customer_id", "region", "amount"))
                .reduceByKey(
                        record -> new Record(record.getField(2)),
                        (left, right) -> left)
                    .withSqlUdfs("region", "SUM(amount) AS total_amount")
                .sort(record -> new Record(record.getField(0)))
                    .withSqlUdf("region", "ASC")
                .writeTable(SINK_TABLE, "overwrite", new String[]{"region", "total_amount"}, new Properties());

        assertEquals("AMER,APAC,EMEA", queryString(
                "SELECT string_agg(region, ',' ORDER BY region) FROM " + SINK_TABLE));
    }

    @Test
    @Order(13)
    void javaPlanBuilderReadTableFilterProjectionTableSink() {
        new JavaPlanBuilder(wayangContext(), "DuckDB JavaPlanBuilder table sink integration test")
                .readTable(new DuckDBTableSource(
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

        assertEquals(3, queryLong("SELECT count(*) FROM " + SINK_TABLE));
        assertEquals(2, queryLong(
                "SELECT count(*) FROM information_schema.columns "
                        + "WHERE table_schema = '" + SCHEMA + "' AND table_name = '" + SINK_TABLE_NAME + "'"));
    }

    @Test
    @Order(14)
    void javaPlanBuilderReadTableJoin() {
        JavaPlanBuilder plan = new JavaPlanBuilder(
                wayangContext(), "DuckDB JavaPlanBuilder join integration test");
        DataQuantaBuilder<?, Record> orders = plan.readTable(new DuckDBTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount"));
        DataQuantaBuilder<?, Record> customers = plan.readTable(new DuckDBTableSource(
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

        assertEquals(6, queryLong("SELECT count(*) FROM " + SINK_TABLE));
        assertEquals(0, queryLong("SELECT SUM(CASE WHEN customer_id <> cust_id THEN 1 ELSE 0 END) FROM "
                + SINK_TABLE));
    }

    @Test
    @Order(15)
    void generatedSqlContainsPushdownShapes() {
        String filterSql = captureStdout(this::filter);
        assertTrue(filterSql.contains("WHERE region = 'AMER'"));

        String projectionSql = captureStdout(this::projection);
        assertTrue(projectionSql.contains("SELECT region, amount FROM " + ORDERS));
        assertTrue(projectionSql.contains("WHERE region = 'AMER'"));

        String joinSql = captureStdout(this::join);
        assertTrue(joinSql.contains("JOIN " + CUSTOMERS));
        assertTrue(joinSql.contains(CUSTOMERS + ".cust_id=" + ORDERS + ".customer_id"));

        String reduceBySql = captureStdout(this::reduceBy);
        assertTrue(reduceBySql.contains("GROUP BY region"));

        String sortSql = captureStdout(this::sort);
        assertTrue(sortSql.contains("ORDER BY amount ASC"));
    }

    private WayangContext wayangContext() {
        Configuration config = new Configuration();
        config.setProperty("wayang.duckdb.jdbc.url", jdbcUrl);
        config.setProperty("wayang.duckdb.jdbc.user", "");
        config.setProperty("wayang.duckdb.jdbc.password", "");
        config.getMappingProvider().addAllToWhitelist(
                Collections.singleton(new JoinFlattenMapping()));
        return new WayangContext(config)
                .withPlugin(DuckDB.plugin());
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
        try (Connection connection = jdbc();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getLong(1);
        } catch (Exception e) {
            throw new RuntimeException("query failed: " + sql, e);
        }
    }

    private double queryDouble(String sql) {
        try (Connection connection = jdbc();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getDouble(1);
        } catch (Exception e) {
            throw new RuntimeException("query failed: " + sql, e);
        }
    }

    private String queryString(String sql) {
        try (Connection connection = jdbc();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getString(1);
        } catch (Exception e) {
            throw new RuntimeException("query failed: " + sql, e);
        }
    }

    private static String captureStdout(Runnable runnable) {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (PrintStream capture = new PrintStream(buffer, true, StandardCharsets.UTF_8)) {
            System.setOut(capture);
            runnable.run();
        } finally {
            System.setOut(originalOut);
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    private void assertSingleDoubleResult(double expected) {
        try (Connection connection = jdbc();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM " + SINK_TABLE)) {
            assertTrue(resultSet.next());
            assertEquals(expected, resultSet.getDouble(1), 0.01);
            assertFalse(resultSet.next());
        } catch (Exception e) {
            throw new RuntimeException("query failed: SELECT * FROM " + SINK_TABLE, e);
        }
    }

    private Map<String, Double> readRegionSums() {
        Map<String, Double> sums = new HashMap<>();
        try (Connection connection = jdbc();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM " + SINK_TABLE)) {
            while (resultSet.next()) {
                sums.put(resultSet.getString(1), resultSet.getDouble(2));
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
                    (matchedOperator, epoch) -> createDuckDBProjection().at(epoch));

            return Collections.singleton(new PlanTransformation(
                    SubplanPattern.createSingleton(pattern),
                    factory,
                    DuckDBPlatform.getInstance()));
        }

        private static DuckDBProjectionOperator createDuckDBProjection() {
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
            return new DuckDBProjectionOperator((MapOperator<Record, Record>) (MapOperator) projection);
        }
    }

    private static Connection jdbc() throws Exception {
        return DriverManager.getConnection(jdbcUrl);
    }

    private static String normalizeDuckDbUrl(String url) {
        String prefix = "jdbc:duckdb:";
        if (!url.startsWith(prefix) || url.length() == prefix.length()) {
            return url;
        }

        String databasePath = url.substring(prefix.length());
        Path path = Path.of(databasePath);
        if (path.isAbsolute()) {
            return url;
        }

        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve(path).normalize();
            if (Files.exists(candidate)) {
                return prefix + candidate;
            }
            current = current.getParent();
        }

        return prefix + Path.of("").toAbsolutePath().resolve(path).normalize();
    }
}
