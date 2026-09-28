/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;

import java.util.Collection;

/**
 * Hackathon smoke test: builds a single Wayang plan that reads from the
 * seeded Postgres table, forces the filter step onto the Java platform,
 * and forces the map step onto the Spark platform, proving all three
 * platforms work together inside the container.
 *
 * Run via hackathon-CoC2026/verify.sh, which sets up the classpath needed
 * for the Postgres and Spark platforms and compiles/runs this class.
 */
public class PostgresSmokeTest {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.setProperty(
                "wayang.postgres.jdbc.url",
                envOrDefault("WAYANG_POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/wayang_hackathon"));
        configuration.setProperty(
                "wayang.postgres.jdbc.user",
                envOrDefault("WAYANG_POSTGRES_USER", "wayang"));
        configuration.setProperty(
                "wayang.postgres.jdbc.password",
                envOrDefault("WAYANG_POSTGRES_PASSWORD", "wayang"));

        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Hackathon smoke test (Postgres + Java + Spark)")
                .withUdfJarOf(PostgresSmokeTest.class);

        Collection<String> result = planBuilder
                .readTable(new PostgresTableSource("word_counts_seed", "word", "count"))
                .filter(record -> record.getInt(1) > 5)
                .withTargetPlatform(Java.platform())
                .map(record -> record.getString(0).toUpperCase() + " -> " + record.getInt(1))
                .withTargetPlatform(Spark.platform())
                .collect();

        System.out.println("Wayang plan executed across Postgres, Java, and Spark. Results:");
        result.forEach(System.out::println);
        System.out.println("Total rows: " + result.size());

        if (result.isEmpty()) {
            throw new IllegalStateException(
                    "Connected and ran fine, but got zero rows back — check that init.sql actually ran " +
                    "(docker compose down -v to reset the postgres volume and retry).");
        }
    }

    private static String envOrDefault(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isEmpty()) ? fallback : value;
    }
}
