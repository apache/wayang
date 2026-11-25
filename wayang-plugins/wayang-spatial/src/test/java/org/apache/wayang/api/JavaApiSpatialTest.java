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

package org.apache.wayang.api;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spatial.Spatial;
import org.apache.wayang.spatial.data.WayangGeometry;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the fluent spatial API on DataQuantaBuilder.
 */
public class JavaApiSpatialTest {

    // ==================== Java Platform Tests ====================

    @Test
    void testSpatialFilter() {
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spatial.javaPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Spatial Filter Test");

        List<String> testData = Arrays.asList(
                "0.0,0.0,1.0,1.0",      // Box at origin
                "0.5,0.5,1.5,1.5",      // Overlapping box
                "2.0,2.0,3.0,3.0",      // Non-overlapping box
                "0.25,0.25,0.75,0.75"   // Box inside first
        );

        WayangGeometry queryGeometry = WayangGeometry.fromStringInput(
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
        );

        Collection<Long> result = planBuilder.loadCollection(testData)
                .spatialFilter(
                        (input -> {
                            String[] parts = input.split(",");
                            double xmin = Double.parseDouble(parts[0]);
                            double ymin = Double.parseDouble(parts[1]);
                            double xmax = Double.parseDouble(parts[2]);
                            double ymax = Double.parseDouble(parts[3]);
                            String wkt = String.format(
                                    "POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))",
                                    xmin, ymin, xmax, ymin, xmax, ymax, xmin, ymax, xmin, ymin
                            );
                            return WayangGeometry.fromStringInput(wkt);
                        }),
                        SpatialPredicate.INTERSECTS,
                        queryGeometry
                )
                .count()
                .collect();

        // Should match 3 boxes (first overlaps, second overlaps, fourth is inside)
        assertEquals(1, result.size());
        Long count = result.iterator().next();
        assertEquals(3L, count);
    }

    @Test
    void testSpatialJoin() {
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spatial.javaPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Spatial Join Test");

        List<String> leftData = Arrays.asList(
                "POINT(0.5 0.5)",  // Inside first box
                "POINT(1.5 1.5)",  // Inside second box
                "POINT(0.25 0.75)" // Inside first box
        );

        List<String> rightData = Arrays.asList(
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",  // Contains first and third points
                "POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))"   // Contains second point
        );

        Collection<Long> result = planBuilder.loadCollection(leftData)
                .spatialJoin(
                        (WayangGeometry::fromStringInput),
                        planBuilder.loadCollection(rightData),
                        (WayangGeometry::fromStringInput),
                        SpatialPredicate.INTERSECTS
                )
                .count()
                .collect();

        // Should have 3 matches:
        // - POINT(0.5 0.5) with first box
        // - POINT(1.5 1.5) with second box
        // - POINT(0.25 0.75) with first box
        assertEquals(1, result.size());
        Long count = result.iterator().next();
        assertEquals(3L, count);
    }

    @Test
    void testChainedOperations() {
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spatial.javaPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Chained Operations Test");

        // Data: "id,xmin,ymin,xmax,ymax"
        List<String> testData = Arrays.asList(
                "1,0.0,0.0,1.0,1.0",
                "2,0.5,0.5,1.5,1.5",
                "3,2.0,2.0,3.0,3.0",
                "4,0.25,0.25,0.75,0.75"
        );

        WayangGeometry queryGeometry = WayangGeometry.fromStringInput(
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
        );

        // Chain: spatialFilter -> map (extract id) -> filter (id > 1) -> collect
        Collection<Integer> result = planBuilder.loadCollection(testData)
                .spatialFilter(
                        (input -> {
                            String[] parts = input.split(",");
                            double xmin = Double.parseDouble(parts[1]);
                            double ymin = Double.parseDouble(parts[2]);
                            double xmax = Double.parseDouble(parts[3]);
                            double ymax = Double.parseDouble(parts[4]);
                            String wkt = String.format(
                                    "POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))",
                                    xmin, ymin, xmax, ymin, xmax, ymax, xmin, ymax, xmin, ymin
                            );
                            return WayangGeometry.fromStringInput(wkt);
                        }),
                        SpatialPredicate.INTERSECTS,
                        queryGeometry
                )
                .map(line -> Integer.parseInt(line.split(",")[0]))  // Extract ID
                .filter(id -> id > 1)  // Keep only IDs > 1
                .collect();

        // Should match boxes 1, 2, 4 (intersect), then filter to IDs > 1 -> 2, 4
        assertEquals(2, result.size());
        assertTrue(result.contains(2));
        assertTrue(result.contains(4));
    }

    @Test
    void testSpatialJoinChainedWithMapAndReduce() {
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spatial.javaPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Spatial Join Chained Test");

        // Left data: points with values "wkt;value"
        List<String> leftData = Arrays.asList(
                "POINT(0.5 0.5);10",
                "POINT(1.5 1.5);20",
                "POINT(0.25 0.75);30"
        );

        // Right data: boxes with multipliers "wkt;multiplier"
        List<String> rightData = Arrays.asList(
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0));2",
                "POLYGON((1 1, 2 1, 2 2, 1 2, 1 1));3"
        );

        // Chain: spatialJoin -> map (multiply values) -> reduce (sum)
        Collection<Integer> result = planBuilder.loadCollection(leftData)
                .spatialJoin(
                        (input -> WayangGeometry.fromStringInput(input.split(";")[0])),
                        planBuilder.loadCollection(rightData),
                        (input -> WayangGeometry.fromStringInput(input.split(";")[0])),
                        SpatialPredicate.INTERSECTS
                )
                .map(tuple -> {
                    int leftValue = Integer.parseInt(tuple.field0.split(";")[1]);
                    int rightMultiplier = Integer.parseInt(tuple.field1.split(";")[1]);
                    return leftValue * rightMultiplier;
                })
                .reduce((a, b) -> a + b)
                .collect();

        // Matches:
        // - POINT(0.5 0.5);10 with box;2 -> 10*2 = 20
        // - POINT(1.5 1.5);20 with box;3 -> 20*3 = 60
        // - POINT(0.25 0.75);30 with box;2 -> 30*2 = 60
        // Sum = 20 + 60 + 60 = 140
        assertEquals(1, result.size());
        assertEquals(140, result.iterator().next());
    }

    @Test
    void testChainedSpatialFilters() {
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spatial.javaPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Chained Spatial Filters Test");

        List<String> testData = Arrays.asList(
                "POLYGON((0.1 0.1, 0.3 0.1, 0.3 0.3, 0.1 0.3, 0.1 0.1))",  // Inside both query geometries
                "POLYGON((0.6 0.6, 0.8 0.6, 0.8 0.8, 0.6 0.8, 0.6 0.6))",  // Inside first, outside second
                "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))"                        // Outside both
        );

        WayangGeometry queryGeometry1 = WayangGeometry.fromStringInput(
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"  // Unit square
        );
        WayangGeometry queryGeometry2 = WayangGeometry.fromStringInput(
                "POLYGON((0 0, 0.5 0, 0.5 0.5, 0 0.5, 0 0))"  // Smaller square (0-0.5 range)
        );

        // Chain two spatial filters
        Collection<Long> result = planBuilder.loadCollection(testData)
                .spatialFilter(
                        (WayangGeometry::fromStringInput),
                        SpatialPredicate.INTERSECTS,
                        queryGeometry1
                )
                .map(x -> x).withOutputClass(String.class)  // Preserve type for chaining
                .spatialFilter(
                        (WayangGeometry::fromStringInput),
                        SpatialPredicate.INTERSECTS,
                        queryGeometry2
                )
                .count()
                .collect();

        // Only the first box (0.1-0.3) should pass both filters
        assertEquals(1, result.size());
        assertEquals(1L, result.iterator().next());
    }

    @Test
    void testSpatialFilterFollowedBySpatialJoin() {
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spatial.javaPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Spatial Filter then Join Test");

        List<String> leftData = Arrays.asList(
                "POINT(0.5 0.5)",
                "POINT(1.5 1.5)",
                "POINT(0.25 0.25)"
        );

        List<String> rightData = Arrays.asList(
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
                "POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))"
        );

        WayangGeometry preFilterGeometry = WayangGeometry.fromStringInput(
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
        );

        // Filter left data, then join with right
        var filteredLeft = planBuilder.loadCollection(leftData)
                .spatialFilter(
                        (input -> WayangGeometry.fromStringInput(input)),
                        SpatialPredicate.INTERSECTS,
                        preFilterGeometry
                )
                .map(x -> x).withOutputClass(String.class);

        Collection<Long> result = filteredLeft
                .spatialJoin(
                        (input -> WayangGeometry.fromStringInput(input)),
                        planBuilder.loadCollection(rightData),
                        (input -> WayangGeometry.fromStringInput(input)),
                        SpatialPredicate.INTERSECTS
                )
                .count()
                .collect();

        // After filter: POINT(0.5 0.5) and POINT(0.25 0.25) remain
        // Join matches: both with first box = 2 matches
        assertEquals(1, result.size());
        assertEquals(2L, result.iterator().next());
    }

    // ==================== Spark Platform Tests ====================

    @Test
    void testSpatialFilterWithSpark() {
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Spatial.sparkPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Spatial Filter Spark Test");

        List<String> testData = Arrays.asList(
                "0.0,0.0,1.0,1.0",
                "0.5,0.5,1.5,1.5",
                "2.0,2.0,3.0,3.0",
                "0.25,0.25,0.75,0.75"
        );

        WayangGeometry queryGeometry = WayangGeometry.fromStringInput(
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
        );

        Collection<Long> result = planBuilder.loadCollection(testData)
                .spatialFilter(
                        (input -> {
                            String[] parts = input.split(",");
                            double xmin = Double.parseDouble(parts[0]);
                            double ymin = Double.parseDouble(parts[1]);
                            double xmax = Double.parseDouble(parts[2]);
                            double ymax = Double.parseDouble(parts[3]);
                            String wkt = String.format(
                                    "POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))",
                                    xmin, ymin, xmax, ymin, xmax, ymax, xmin, ymax, xmin, ymin
                            );
                            return WayangGeometry.fromStringInput(wkt);
                        }),
                        SpatialPredicate.INTERSECTS,
                        queryGeometry
                )
                .withTargetPlatform(Spark.platform())
                .count()
                .collect();

        assertEquals(1, result.size());
        assertEquals(3L, result.iterator().next());
    }

    @Test
    void testSpatialJoinWithSpark() {
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Spatial.sparkPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Spatial Join Spark Test");

        List<String> leftData = Arrays.asList(
                "POINT(0.5 0.5)",
                "POINT(1.5 1.5)",
                "POINT(0.25 0.75)"
        );

        List<String> rightData = Arrays.asList(
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
                "POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))"
        );

        Collection<Long> result = planBuilder.loadCollection(leftData)
                .spatialJoin(
                        (input -> WayangGeometry.fromStringInput(input)),
                        planBuilder.loadCollection(rightData),
                        (input -> WayangGeometry.fromStringInput(input)),
                        SpatialPredicate.INTERSECTS
                )
                .withTargetPlatform(Spark.platform())
                .count()
                .collect();

        assertEquals(1, result.size());
        assertEquals(3L, result.iterator().next());
    }

    @Test
    void testSpatialFilterWithJavaAndSpark() {
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Spatial.plugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Spatial Filter Java+Spark Test");

        List<String> testData = Arrays.asList(
                "0.0,0.0,1.0,1.0",
                "0.5,0.5,1.5,1.5",
                "2.0,2.0,3.0,3.0",
                "0.25,0.25,0.75,0.75"
        );

        WayangGeometry queryGeometry = WayangGeometry.fromStringInput(
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
        );

        // Let Wayang choose the platform
        Collection<Long> result = planBuilder.loadCollection(testData)
                .spatialFilter(
                        (input -> {
                            String[] parts = input.split(",");
                            double xmin = Double.parseDouble(parts[0]);
                            double ymin = Double.parseDouble(parts[1]);
                            double xmax = Double.parseDouble(parts[2]);
                            double ymax = Double.parseDouble(parts[3]);
                            String wkt = String.format(
                                    "POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))",
                                    xmin, ymin, xmax, ymin, xmax, ymax, xmin, ymax, xmin, ymin
                            );
                            return WayangGeometry.fromStringInput(wkt);
                        }),
                        SpatialPredicate.INTERSECTS,
                        queryGeometry
                )
                .count()
                .collect();

        assertEquals(1, result.size());
        assertEquals(3L, result.iterator().next());
    }

    // ==================== PostgreSQL Platform Tests ====================
    // These tests use PostgreSQL spatial operators with ST_Intersects pushdown.

    /**
     * Helper method to create a PostgreSQL-configured WayangContext.
     * Connects to spiderdb on localhost:5433.
     */
    private Configuration getPostgresConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5433/spiderdb");
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "postgres");
        return configuration;
    }

    @Test
    @Disabled("Requires local Postgres test database.")
    void testSpatialFilterWithPostgres() {
        Configuration configuration = getPostgresConfiguration();

        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.postgresPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Spatial Filter with Postgres Test");

        // Query geometry: a box in the lower-left quadrant (0,0) to (0.4, 0.4)
        WayangGeometry queryGeometry = WayangGeometry.fromStringInput(
                "POLYGON((0.0 0.0, 0.4 0.0, 0.4 0.4, 0.0 0.4, 0.0 0.0))"
        );

        // Read from spider_boxes table and apply spatial filter using PostgreSQL ST_Intersects
        Collection<Long> result = planBuilder
                .readTable(new PostgresTableSource("spider_boxes", "x_min", "y_min", "x_max", "y_max", "geom"))
                .spatialFilter(
                        (Record record) -> WayangGeometry.fromStringInput(record.getString(4)),
                        SpatialPredicate.INTERSECTS,
                        queryGeometry,
                        "geom"  // SQL geometry column name for PostgreSQL pushdown
                )
                .withTargetPlatform(Postgres.platform())
                .count()
                .collect();

        // Verify we got results (exact count depends on data in spider_boxes)
        assertEquals(1, result.size());
        Long count = result.iterator().next();
        assertTrue(count > 0, "Expected at least one box intersecting the query geometry");
        System.out.println("PostgreSQL Spatial Filter (ST_Intersects): " + count + " boxes intersect the query geometry");
    }

    @Test
    @Disabled("Requires local Postgres test database.")
    void testSpatialFilterWithPostgresAndMapping() {
        Configuration configuration = getPostgresConfiguration();

        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.postgresPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Spatial Filter with Postgres and Mapping Test");

        // Query geometry covering center area
        WayangGeometry queryGeometry = WayangGeometry.fromStringInput(
                "POLYGON((0.3 0.3, 0.7 0.3, 0.7 0.7, 0.3 0.7, 0.3 0.3))"
        );

        // Read from spider_boxes, filter spatially with PostgreSQL, then map to extract bounds
        Collection<String> result = planBuilder
                .readTable(new PostgresTableSource("spider_boxes", "x_min", "y_min", "x_max", "y_max", "geom"))
                .spatialFilter(
                        (Record record) -> WayangGeometry.fromStringInput(record.getString(4)),
                        SpatialPredicate.INTERSECTS,
                        queryGeometry,
                        "geom"  // SQL geometry column name for PostgreSQL pushdown
                )
                .withTargetPlatform(Postgres.platform())
                .map((Record record) -> String.format("Box: (%.2f,%.2f)-(%.2f,%.2f)",
                        record.getDouble(0), record.getDouble(1),
                        record.getDouble(2), record.getDouble(3)))
                .collect();

        assertTrue(result.size() > 0, "Expected at least one box intersecting the query geometry");
        System.out.println("PostgreSQL Spatial Filter + Mapping: " + result.size() + " results");
        result.stream().limit(5).forEach(System.out::println);
    }

    @Test
    @Disabled("Requires local Postgres test database.")
    void testSpatialFilterWithPostgresContains() {
        Configuration configuration = getPostgresConfiguration();

        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.postgresPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Spatial Filter with Postgres Contains Test");

        // Query geometry: full unit square - should contain all boxes that are fully inside
        WayangGeometry queryGeometry = WayangGeometry.fromStringInput(
                "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))"
        );

        // Test WITHIN predicate - find boxes that are completely within the query geometry
        Collection<Long> result = planBuilder
                .readTable(new PostgresTableSource("spider_boxes", "x_min", "y_min", "x_max", "y_max", "geom"))
                .spatialFilter(
                        (Record record) -> WayangGeometry.fromStringInput(record.getString(4)),
                        SpatialPredicate.WITHIN,
                        queryGeometry,
                        "geom"  // SQL geometry column name for PostgreSQL pushdown
                )
                .withTargetPlatform(Postgres.platform())
                .count()
                .collect();

        assertEquals(1, result.size());
        Long count = result.iterator().next();
        System.out.println("PostgreSQL Spatial Filter (ST_Within): " + count + " boxes within the query geometry");
    }

    @Test
    @Disabled("Requires local Postgres test database.")
    void testSpatialJoinWithPostgres() {
        WayangContext wayangContext = new WayangContext(getPostgresConfiguration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.postgresPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Spatial Join with Postgres Test");

        // Joins two distinct tables: spider_boxes_copy is a copy of spider_boxes.
        Collection<Long> result = planBuilder
                .readTable(new PostgresTableSource("spider_boxes", "x_min", "y_min", "x_max", "y_max", "geom"))
                .spatialJoin(
                        (Record record) -> WayangGeometry.fromStringInput(record.getString(4)),
                        planBuilder.readTable(
                                new PostgresTableSource("spider_boxes_copy", "x_min", "y_min", "x_max", "y_max", "geom")),
                        (Record record) -> WayangGeometry.fromStringInput(record.getString(4)),
                        SpatialPredicate.INTERSECTS
                )
                .withSqlGeometryColumns("spider_boxes", "geom", "spider_boxes_copy", "geom")
                .withTargetPlatform(Postgres.platform())
                .count()
                .collect();

        assertEquals(1, result.size());
        assertTrue(result.iterator().next() > 0, "Expected every box to intersect at least its own copy.");
    }

    @Test
    void testSpatialJoinRejectsPartialSqlGeometryColumns() {
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spatial.javaPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext);

        assertThrows(IllegalArgumentException.class, () -> planBuilder
                .loadCollection(Arrays.asList("POINT(0.5 0.5)"))
                .spatialJoin(
                        WayangGeometry::fromStringInput,
                        planBuilder.loadCollection(Arrays.asList("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")),
                        WayangGeometry::fromStringInput,
                        SpatialPredicate.INTERSECTS
                )
                .withSqlGeometryColumns("points", "geom", null, null)
                .count()
                .collect());
    }

    // ==================== GeoJSON Source / Sink ====================

    @Test
    void testGeoJsonRoundTrip(@TempDir Path tempDir) throws Exception {
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spatial.javaPlugin());

        Path outputFile = tempDir.resolve("features.geojson");
        String outputUrl = "file:" + outputFile;

        Map<String, Object> pointProperties = new HashMap<>();
        pointProperties.put("name", "alpha");
        Map<String, Object> polygonProperties = new HashMap<>();
        polygonProperties.put("name", "beta");

        List<Record> features = Arrays.asList(
                new Record(WayangGeometry.fromStringInput("POINT(1 2)"), pointProperties),
                new Record(
                        WayangGeometry.fromStringInput("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"),
                        polygonProperties)
        );

        new JavaPlanBuilder(wayangContext)
                .withJobName("GeoJSON Sink Test")
                .loadCollection(features)
                .writeGeoJsonFile(outputUrl);

        assertTrue(Files.exists(outputFile), "Expected the GeoJSON output file to be created.");
        assertTrue(Files.readString(outputFile).startsWith("{\"type\":\"FeatureCollection\""),
                "Expected a GeoJSON FeatureCollection.");

        Collection<Record> readBack = new JavaPlanBuilder(wayangContext)
                .withJobName("GeoJSON Source Test")
                .readGeoJsonFile(outputUrl)
                .collect();

        assertEquals(2, readBack.size(), "Expected both features to be read back.");

        // The sink and the source agree on the record shape: geometry in field 0, properties in field 1.
        List<String> geometryTypes = readBack.stream()
                .map(record -> ((WayangGeometry) record.getField(0)).getGeometry().getGeometryType())
                .sorted()
                .collect(Collectors.toList());
        assertEquals(Arrays.asList("Point", "Polygon"), geometryTypes,
                "Expected the geometries to survive the round trip.");

        List<String> names = readBack.stream()
                .map(record -> ((Map<?, ?>) record.getField(1)).get("name").toString())
                .sorted()
                .collect(Collectors.toList());
        assertEquals(Arrays.asList("alpha", "beta"), names,
                "Expected the feature properties to survive the round trip.");
    }

    @Test
    void testGeoJsonNullPropertiesAndNoCrs(@TempDir Path tempDir) throws Exception {
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spatial.javaPlugin());

        Path outputFile = tempDir.resolve("null-properties.geojson");
        String outputUrl = "file:" + outputFile;

        new JavaPlanBuilder(wayangContext)
                .withJobName("GeoJSON Sink Null Properties Test")
                .loadCollection(Arrays.asList(new Record(WayangGeometry.fromStringInput("POINT(1 2)"), null)))
                .writeGeoJsonFile(outputUrl);

        String written = Files.readString(outputFile);
        assertFalse(written.contains("\"crs\""), "Expected no crs member, as RFC 7946 requires: " + written);
        assertTrue(written.contains("\"properties\":null"), "Expected null properties to be kept: " + written);

        Collection<Record> readBack = new JavaPlanBuilder(wayangContext)
                .withJobName("GeoJSON Source Null Properties Test")
                .readGeoJsonFile(outputUrl)
                .collect();

        assertEquals(1, readBack.size());
        Record feature = readBack.iterator().next();
        assertEquals("Point", ((WayangGeometry) feature.getField(0)).getGeometry().getGeometryType());
        assertNull(feature.getField(1), "Expected the null properties to survive the round trip.");
    }

}
