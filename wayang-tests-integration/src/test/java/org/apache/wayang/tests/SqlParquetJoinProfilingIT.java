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
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ParquetSource;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.bigquery.BigQuery;
import org.apache.wayang.bigquery.operators.BigQueryProjectionOperator;
import org.apache.wayang.bigquery.platform.BigQueryPlatform;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.presto.Presto;
import org.apache.wayang.presto.operators.PrestoProjectionOperator;
import org.apache.wayang.presto.platform.PrestoPlatform;
import org.apache.wayang.trino.Trino;
import org.apache.wayang.trino.operators.TrinoProjectionOperator;
import org.apache.wayang.trino.platform.TrinoPlatform;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Real-service smoke test for Parquet-backed SQL join profiling.
 *
 * <p>The test runs on a single target SQL platform at a time. Logical
 * {@link ParquetSource}s are mapped to relations that the selected platform can
 * read from Parquet.
 */
class SqlParquetJoinProfilingIT {

    private static final String TARGET_PLATFORM = cfg(
            "wayang.profile.parquet.target",
            "WAYANG_PROFILE_PARQUET_TARGET",
            "trino"
    ).toLowerCase(Locale.ROOT);
    private static final String ORDERS_URI = cfg(
            "wayang.profile.parquet.orders.uri",
            "WAYANG_PROFILE_PARQUET_ORDERS_URI",
            ""
    );
    private static final String CUSTOMERS_URI = cfg(
            "wayang.profile.parquet.customers.uri",
            "WAYANG_PROFILE_PARQUET_CUSTOMERS_URI",
            ""
    );
    private static final long EXPECTED_ROWS = Long.parseLong(cfg(
            "wayang.profile.parquet.expectedRows",
            "WAYANG_PROFILE_PARQUET_EXPECTED_ROWS",
            "-1"
    ));
    private static final String JOIN_FLATTEN_NAME = "SQL Parquet profile join flatten";
    private static final String[] ORDERS_COLUMNS = {"order_id", "customer_id", "region", "amount"};
    private static final String[] CUSTOMERS_COLUMNS = {"cust_id", "tier"};
    private static final String[] JOIN_COLUMNS = {
            "order_id", "customer_id", "region", "amount", "cust_id", "tier"
    };
    private static final Path OUTPUT_DIR = Paths.get(cfg(
            "wayang.profile.parquet.outputDir",
            "WAYANG_PROFILE_PARQUET_OUTPUT_DIR",
            "target/cost-profiling/parquet-sql"
    ));
    private static final Path EXECUTIONS_PATH = OUTPUT_DIR.resolve("executions.json");
    private static final Path CARDINALITIES_PATH = OUTPUT_DIR.resolve("cardinalities.json");
    private static final Path MANIFEST_PATH = OUTPUT_DIR.resolve("manifest.csv");

    private String sinkRelation;

    @AfterEach
    void tearDown() throws Exception {
        if (this.sinkRelation != null && isCleanupEnabled()) {
            dropRelation(TARGET_PLATFORM, this.sinkRelation);
        }
    }

    @Test
    void runsParquetJoinProfilingOnTargetSqlPlatform() throws Exception {
        Assumptions.assumeTrue(isEnabled(), "Set wayang.profile.parquet.enabled=true to run this test.");
        Assumptions.assumeFalse(ORDERS_URI.isEmpty(), "Configure wayang.profile.parquet.orders.uri.");
        Assumptions.assumeFalse(CUSTOMERS_URI.isEmpty(), "Configure wayang.profile.parquet.customers.uri.");
        Assumptions.assumeTrue(isSupportedTarget(TARGET_PLATFORM), "Unsupported target platform: " + TARGET_PLATFORM);

        this.sinkRelation = relation(TARGET_PLATFORM, "sink");
        Assumptions.assumeFalse(this.sinkRelation.isEmpty(),
                "Configure wayang.profile.parquet." + TARGET_PLATFORM + ".sink.relation.");
        Assumptions.assumeFalse(relation(TARGET_PLATFORM, "orders").isEmpty(),
                "Configure wayang.profile.parquet." + TARGET_PLATFORM + ".orders.relation.");
        Assumptions.assumeFalse(relation(TARGET_PLATFORM, "customers").isEmpty(),
                "Configure wayang.profile.parquet." + TARGET_PLATFORM + ".customers.relation.");
        Assumptions.assumeTrue(isPlatformAvailable(TARGET_PLATFORM),
                "Target platform is not reachable: " + TARGET_PLATFORM);

        Files.createDirectories(OUTPUT_DIR);
        writeManifestHeaderIfNeeded();
        dropRelation(TARGET_PLATFORM, this.sinkRelation);

        Platform targetPlatform = targetPlatform();
        WayangPlan plan = createParquetJoinPlan(this.sinkRelation, targetPlatform);
        long startedAt = System.currentTimeMillis();
        wayangContext(targetPlatform).execute("SQL-Parquet-Join-Profiling-" + TARGET_PLATFORM, plan);
        long elapsedMillis = System.currentTimeMillis() - startedAt;

        long actualRows = queryLong(TARGET_PLATFORM, "SELECT count(*) FROM " + this.sinkRelation);
        if (EXPECTED_ROWS >= 0) {
            assertEquals(EXPECTED_ROWS, actualRows);
        }
        appendManifest(TARGET_PLATFORM, actualRows, elapsedMillis, this.sinkRelation);
    }

    private static WayangPlan createParquetJoinPlan(String sinkRelation, Platform targetPlatform) {
        ParquetSource orders = new ParquetSource(ORDERS_URI, null, ORDERS_COLUMNS);
        ParquetSource customers = new ParquetSource(CUSTOMERS_URI, null, CUSTOMERS_COLUMNS);
        JoinOperator<Record, Record, Record> join = new JoinOperator<>(
                new TransformationDescriptor<>(
                        record -> new Record(record.getField(1)),
                        Record.class,
                        Record.class
                ).withSqlImplementation(ORDERS_URI, "customer_id"),
                new TransformationDescriptor<>(
                        record -> new Record(record.getField(0)),
                        Record.class,
                        Record.class
                ).withSqlImplementation(CUSTOMERS_URI, "cust_id")
        );
        MapOperator<Tuple2<Record, Record>, Record> flatten = joinFlattenOperator();
        TableSink<Record> sink = new TableSink<>(new Properties(), "overwrite", sinkRelation, JOIN_COLUMNS);

        orders.addTargetPlatform(targetPlatform);
        customers.addTargetPlatform(targetPlatform);
        join.addTargetPlatform(targetPlatform);
        flatten.addTargetPlatform(targetPlatform);
        sink.addTargetPlatform(targetPlatform);

        orders.connectTo(0, join, 0);
        customers.connectTo(0, join, 1);
        join.connectTo(0, flatten, 0);
        flatten.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }

    private static WayangContext wayangContext(Platform targetPlatform) {
        Configuration configuration = new Configuration();
        configureJdbc(configuration, TARGET_PLATFORM);
        configureParquetMappings(configuration, TARGET_PLATFORM);
        configuration.setProperty("wayang.core.log.enabled", "true");
        configuration.setProperty("wayang.core.explain.enabled", "false");
        configuration.setProperty("wayang.core.log.executions", EXECUTIONS_PATH.toString().replace('\\', '/'));
        configuration.setProperty("wayang.core.log.cardinalities", CARDINALITIES_PATH.toString().replace('\\', '/'));
        configuration.getMappingProvider().addAllToWhitelist(Collections.singleton(new JoinFlattenMapping(targetPlatform)));

        WayangContext context = new WayangContext(configuration);
        switch (TARGET_PLATFORM) {
            case "trino":
                return context.withPlugin(Trino.plugin());
            case "presto":
                return context.withPlugin(Presto.plugin());
            case "bigquery":
                return context.withPlugin(BigQuery.plugin());
            default:
                throw new IllegalArgumentException("Unsupported target platform: " + TARGET_PLATFORM);
        }
    }

    private static void configureJdbc(Configuration configuration, String platform) {
        switch (platform) {
            case "trino":
                configuration.setProperty("wayang.trino.jdbc.url", cfg(
                        "wayang.trino.jdbc.url",
                        "WAYANG_TRINO_JDBC_URL",
                        "jdbc:trino://localhost:8080"
                ));
                configuration.setProperty("wayang.trino.jdbc.user", cfg(
                        "wayang.trino.jdbc.user",
                        "WAYANG_TRINO_JDBC_USER",
                        "admin"
                ));
                configuration.setProperty("wayang.trino.jdbc.password", cfg(
                        "wayang.trino.jdbc.password",
                        "WAYANG_TRINO_JDBC_PASSWORD",
                        ""
                ));
                return;
            case "presto":
                configuration.setProperty("wayang.presto.jdbc.url", cfg(
                        "wayang.presto.jdbc.url",
                        "WAYANG_PRESTO_JDBC_URL",
                        "jdbc:presto://localhost:8081/hive"
                ));
                configuration.setProperty("wayang.presto.jdbc.user", cfg(
                        "wayang.presto.jdbc.user",
                        "WAYANG_PRESTO_JDBC_USER",
                        "test"
                ));
                return;
            case "bigquery":
                configuration.setProperty("wayang.bigquery.jdbc.url", bigQueryJdbcUrl());
                return;
            default:
                throw new IllegalArgumentException("Unsupported target platform: " + platform);
        }
    }

    private static void configureParquetMappings(Configuration configuration, String platform) {
        String ordersRelation = relation(platform, "orders");
        String customersRelation = relation(platform, "customers");
        if (!ordersRelation.isEmpty() && !customersRelation.isEmpty()) {
            configuration.setProperty(
                    String.format("wayang.%s.parquetsource.mappings", platform),
                    ORDERS_URI + "=" + ordersRelation + ";" + CUSTOMERS_URI + "=" + customersRelation
            );
        }
    }

    private static MapOperator<Tuple2<Record, Record>, Record> joinFlattenOperator() {
        MapOperator<Tuple2<Record, Record>, Record> operator = new MapOperator<>(
                new TransformationDescriptor<>(
                        new JoinFlattenFunction(),
                        DataUnitType.createBasicUnchecked(Tuple2.class),
                        DataUnitType.createBasic(Record.class)
                ),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                DataSetType.createDefault(Record.class)
        );
        operator.setName(JOIN_FLATTEN_NAME);
        return operator;
    }

    private static Platform targetPlatform() {
        switch (TARGET_PLATFORM) {
            case "trino":
                return TrinoPlatform.getInstance();
            case "presto":
                return PrestoPlatform.getInstance();
            case "bigquery":
                return BigQueryPlatform.getInstance();
            default:
                throw new IllegalArgumentException("Unsupported target platform: " + TARGET_PLATFORM);
        }
    }

    private static boolean isSupportedTarget(String platform) {
        return "trino".equals(platform) || "presto".equals(platform) || "bigquery".equals(platform);
    }

    private static boolean isPlatformAvailable(String platform) {
        try (Connection connection = jdbc(platform);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT 1")) {
            return resultSet.next();
        } catch (Exception e) {
            return false;
        }
    }

    private static long queryLong(String platform, String sql) throws Exception {
        try (Connection connection = jdbc(platform);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private static void dropRelation(String platform, String relation) throws Exception {
        try (Connection connection = jdbc(platform); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + relation);
        }
    }

    private static Connection jdbc(String platform) throws Exception {
        switch (platform) {
            case "trino":
                return DriverManager.getConnection(
                        cfg("wayang.trino.jdbc.url", "WAYANG_TRINO_JDBC_URL", "jdbc:trino://localhost:8080"),
                        cfg("wayang.trino.jdbc.user", "WAYANG_TRINO_JDBC_USER", "admin"),
                        cfg("wayang.trino.jdbc.password", "WAYANG_TRINO_JDBC_PASSWORD", "")
                );
            case "presto":
                Properties prestoProperties = new Properties();
                prestoProperties.setProperty(
                        "user",
                        cfg("wayang.presto.jdbc.user", "WAYANG_PRESTO_JDBC_USER", "test")
                );
                return DriverManager.getConnection(
                        cfg("wayang.presto.jdbc.url", "WAYANG_PRESTO_JDBC_URL", "jdbc:presto://localhost:8081/hive"),
                        prestoProperties
                );
            case "bigquery":
                Class.forName("com.google.cloud.bigquery.jdbc.BigQueryDriver");
                return DriverManager.getConnection(bigQueryJdbcUrl());
            default:
                throw new IllegalArgumentException("Unsupported platform: " + platform);
        }
    }

    private static String bigQueryJdbcUrl() {
        String configuredUrl = cfg("wayang.bigquery.jdbc.url", "WAYANG_BIGQUERY_JDBC_URL", "");
        if (!configuredUrl.isEmpty()) {
            return configuredUrl;
        }
        String projectId = cfg("bigquery.project", "BIGQUERY_PROJECT", "your-project");
        String serviceAccount = cfg(
                "bigquery.saEmail",
                "BIGQUERY_SA_EMAIL",
                "wayang-bq@" + projectId + ".iam.gserviceaccount.com"
        );
        String keyPath = cfg(
                "bigquery.keyPath",
                "BIGQUERY_KEY_PATH",
                System.getProperty("user.home") + "/wayang-bq-key.json"
        );
        String location = cfg("bigquery.location", "BIGQUERY_LOCATION", "US");
        return String.format(
                "jdbc:bigquery://https://www.googleapis.com/bigquery/v2;"
                        + "ProjectId=%s;OAuthType=0;OAuthServiceAcctEmail=%s;OAuthPvtKeyPath=%s;Location=%s",
                projectId,
                serviceAccount,
                keyPath,
                location
        );
    }

    private static String relation(String platform, String relationRole) {
        return cfg(
                String.format("wayang.profile.parquet.%s.%s.relation", platform, relationRole),
                String.format(
                        "WAYANG_PROFILE_PARQUET_%s_%s_RELATION",
                        platform.toUpperCase(Locale.ROOT),
                        relationRole.toUpperCase(Locale.ROOT)
                ),
                ""
        );
    }

    private static boolean isEnabled() {
        return Boolean.parseBoolean(cfg(
                "wayang.profile.parquet.enabled",
                "WAYANG_PROFILE_PARQUET_ENABLED",
                "false"
        ));
    }

    private static boolean isCleanupEnabled() {
        return Boolean.parseBoolean(cfg(
                "wayang.profile.parquet.cleanup",
                "WAYANG_PROFILE_PARQUET_CLEANUP",
                "true"
        ));
    }

    private static void writeManifestHeaderIfNeeded() throws Exception {
        if (Files.exists(MANIFEST_PATH)) {
            return;
        }
        try (BufferedWriter writer = Files.newBufferedWriter(MANIFEST_PATH, StandardCharsets.UTF_8)) {
            writer.write("target_platform,orders_uri,customers_uri,actual_rows,expected_rows,elapsed_millis,sink_relation");
            writer.newLine();
        }
    }

    private static void appendManifest(String platform,
                                       long actualRows,
                                       long elapsedMillis,
                                       String sinkRelation) throws Exception {
        try (BufferedWriter writer = Files.newBufferedWriter(
                MANIFEST_PATH,
                StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.APPEND)) {
            writer.write(String.join(",",
                    platform,
                    ORDERS_URI,
                    CUSTOMERS_URI,
                    String.valueOf(actualRows),
                    String.valueOf(EXPECTED_ROWS),
                    String.valueOf(elapsedMillis),
                    sinkRelation));
            writer.newLine();
        }
    }

    private static String cfg(String sysProp, String envVar, String dflt) {
        String value = System.getProperty(sysProp);
        if (value == null || value.isEmpty()) {
            value = System.getenv(envVar);
        }
        return value == null || value.isEmpty() ? dflt : value;
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
                right.getField(1)
        );
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

        private final Platform targetPlatform;

        private JoinFlattenMapping(Platform targetPlatform) {
            this.targetPlatform = targetPlatform;
        }

        @Override
        public Collection<PlanTransformation> getTransformations() {
            return Collections.singleton(createTransformation(this.targetPlatform, this::createProjectionOperator));
        }

        private org.apache.wayang.core.plan.wayangplan.Operator createProjectionOperator(
                MapOperator matchedOperator,
                Integer epoch) {
            switch (TARGET_PLATFORM) {
                case "trino":
                    return new TrinoProjectionOperator((MapOperator<Record, Record>) (MapOperator) createProjection()).at(epoch);
                case "presto":
                    return new PrestoProjectionOperator((MapOperator<Record, Record>) (MapOperator) createProjection()).at(epoch);
                case "bigquery":
                    return new BigQueryProjectionOperator((MapOperator<Record, Record>) (MapOperator) createProjection()).at(epoch);
                default:
                    throw new IllegalArgumentException("Unsupported target platform: " + TARGET_PLATFORM);
            }
        }

        private static PlanTransformation createTransformation(
                Platform platform,
                BiFunction<MapOperator, Integer, org.apache.wayang.core.plan.wayangplan.Operator> factoryFunction) {
            OperatorPattern<MapOperator> pattern = new OperatorPattern(
                    "joinFlatten",
                    new MapOperator(null, DataSetType.none(), DataSetType.createDefault(Record.class)),
                    false
            ).withAdditionalTest(operator -> JOIN_FLATTEN_NAME.equals(((MapOperator) operator).getName()));

            ReplacementSubplanFactory factory = new ReplacementSubplanFactory.OfSingleOperators<MapOperator>(
                    factoryFunction::apply
            );

            return new PlanTransformation(SubplanPattern.createSingleton(pattern), factory, platform);
        }

        private static MapOperator<Tuple2<Record, Record>, Record> createProjection() {
            ProjectionDescriptor<Tuple2<Record, Record>, Record> descriptor = new ProjectionDescriptor<>(
                    new JoinFlattenFunction(),
                    java.util.Arrays.asList(JOIN_COLUMNS),
                    DataUnitType.createBasicUnchecked(Tuple2.class),
                    DataUnitType.createBasic(Record.class)
            );
            MapOperator<Tuple2<Record, Record>, Record> projection = new MapOperator<>(
                    descriptor,
                    DataSetType.createDefaultUnchecked(Tuple2.class),
                    DataSetType.createDefault(Record.class)
            );
            projection.setName(JOIN_FLATTEN_NAME);
            return projection;
        }
    }
}
