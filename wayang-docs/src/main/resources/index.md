---
license: |
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
layout: default
title: "Read Me"
menus: header
previous:
    url: /
    title: previous
next:
    url: /
    title: next
menus:
    header:
        weight: 0
---

# Apache Wayang <img align="right" style="margin-top: -0.5em;" width="170px" src="https://wayang.apache.org/assets/img/logo/logo_400x160.png" alt="Wayang logo">

[![Build Status (Travis)](https://travis-ci.org/wayang-ecosystem/wayang.svg?branch=master)](https://travis-ci.org/wayang-ecosystem/wayang)
[![Gitter chat](https://badges.gitter.im/wayang-ecosystem/Lobby.png)](https://gitter.im/wayang-ecosystem/Lobby)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.wayang/wayang/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.apache.wayang/wayang)

#### Apache Wayang - A cross-platform data processing system

Unlike conventional data processing systems that depend on a single execution engine, Apache Wayang acts as a meta processing framework. It empowers you to specify your data processing application through one of its APIs, and Wayang then intelligently chooses the ideal combination of underlying processing frameworks, like Java Streams or Apache Spark, to run your application efficiently. Wayang seamlessly manages inter-platform communication, eliminating the need to grapple with various platform APIs.
   
Wayang has built in support for the following frameworks:

- Java Streams (Java 17)
- Apache Spark v3.5.x
- Apache Flink v1.20.x
- PostgreSQL (JDBC)
- SQLite3 (JDBC)
- Generic JDBC (Relational databases via JDBC)
- Trino
- Presto
- Google BigQuery
- TensorFlow (Deep Learning)
- Apache Giraph v1.2.0

Important note: depending on the scala version the list of the supported platforms available could be different.

## How to use Wayang

### Quick Navigation
- **[How to Build Wayang (Requirements)]({% link getting_start/how_build/index.md %})**: System requirements (Java 17, Scala 2.12, Maven) and source build instructions.
- **[Wayang Abstractions & Plans]({% link getting_start/writting_wayang_plan/index.md %})**: Learn about Source, Unary, Binary, Loop, and Sink operators and how to build plans using `JavaPlanBuilder`.
- **[Configuring Wayang]({% link using_wayang/configuring_wayang.md %})**: System properties, platform parameters, and runtime configuration.
- **[Cost Model Calibration]({% link using_wayang/cost_model_calibration.md %})**: Tuning the cost-based optimizer and calibrating load profile estimators.
- **[Scalable Deep Learning]({% link using_wayang/scalable_deep_learning.md %})**: Deep learning with `DLModel` and the TensorFlow platform adapter.
- **[API JavaDocs](https://wayang.apache.org/docs/api/javadocs/)**: Official API reference documentation.

---

### Get Wayang
Wayang is available via Maven Central. To use it with Maven, include the following into your `pom.xml`:
```xml
<dependency>
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-core</artifactId>
  <version>${wayang.version}</version>
</dependency>
<dependency>
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-basic</artifactId>
  <version>${wayang.version}</version>
</dependency>
<dependency>
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-api-scala-java</artifactId>
  <version>${wayang.version}</version>
</dependency>
```

> Replace `${wayang.version}` with the latest stable release (e.g., `1.1.1` from Maven Central) or the latest development snapshot version (e.g., `1.1.2-SNAPSHOT` with Apache Snapshots repository enabled).

Add the platform adapters you wish to target:
- `wayang-java`: Java Streams platform (single JVM)
- `wayang-spark`: Apache Spark platform
- `wayang-flink`: Apache Flink platform
- `wayang-postgres` / `wayang-sqlite3` / `wayang-generic-jdbc`: Relational database platforms
- `wayang-trino` / `wayang-presto`: Distributed SQL query engines
- `wayang-bigquery`: Google BigQuery cloud data warehouse
- `wayang-tensorflow`: TensorFlow deep learning platform
- `wayang-giraph`: Graph processing platform

For advanced build and configuration options, see [How to Build]({% link getting_start/how_build/index.md %}) and [Configuring Wayang]({% link using_wayang/configuring_wayang.md %}).

## Examples

For some executable examples, have a look at [this repository](https://github.com/sekruse/rheem-examples).

### WordCount

#### Java API
```java
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import java.util.Collection;
import java.util.Arrays;

public class WordcountJava {

    public static void main(String[] args){

        // Settings
        String inputUrl = "file:/tmp.txt";

        // Get a plan builder.
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName(String.format("WordCount (%s)", inputUrl))
                .withUdfJarOf(WordcountJava.class);

        // Start building the WayangPlan.
        Collection<Tuple2<String, Integer>> wordcounts = planBuilder
                // Read the text file.
                .readTextFile(inputUrl).withName("Load file")

                // Split each line by non-word characters.
                .flatMap(line -> Arrays.asList(line.split("\\W+")))
                .withSelectivity(10, 100, 0.9)
                .withName("Split words")

                // Filter empty tokens.
                .filter(token -> !token.isEmpty())
                .withSelectivity(0.99, 0.99, 0.99)
                .withName("Filter empty words")

                // Attach counter to each word.
                .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")

                // Sum up counters for every word.
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withCardinalityEstimator(new DefaultCardinalityEstimator(0.9, 1, false, in -> Math.round(0.01 * in[0])))
                .withName("Add counters")

                // Execute the plan and collect the results.
                .collect();

        System.out.println(wordcounts);
    }
}
```

#### Scala API

```scala
import org.apache.wayang.api._
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark

object WordcountScala {
  def main(args: Array[String]) {

    // Settings
    val inputUrl = "file:/tmp.txt"

    // Get a plan builder.
    val wayangContext = new WayangContext(new Configuration)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Spark.basicPlugin)
    val planBuilder = new PlanBuilder(wayangContext)
      .withJobName(s"WordCount ($inputUrl)")
      .withUdfJarsOf(this.getClass)

    val wordcounts = planBuilder
      // Read the text file.
      .readTextFile(inputUrl).withName("Load file")

      // Split each line by non-word characters.
      .flatMap(_.split("\\W+"), selectivity = 10).withName("Split words")

      // Filter empty tokens.
      .filter(_.nonEmpty, selectivity = 0.99).withName("Filter empty words")

      // Attach counter to each word.
      .map(word => (word.toLowerCase, 1)).withName("To lower case, add counter")

      // Sum up counters for every word.
      .reduceByKey(_._1, (c1, c2) => (c1._1, c1._2 + c2._2)).withName("Add counters")
      .withCardinalityEstimator((in: Long) => math.round(in * 0.01))

      // Execute the plan and collect the results.
      .collect()

    println(wordcounts)
  }
}
```

### k-means

Wayang is also capable of iterative processing, which is, e.g., very important for machine learning algorithms, such as k-means.

#### Scala API

```scala
import org.apache.wayang.api._
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.apache.wayang.core.function.ExecutionContext
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark

import scala.util.Random
import scala.collection.JavaConversions._

object kmeans {
  def main(args: Array[String]) {

    // Settings
    val inputUrl = "file:/kmeans.txt"
    val k = 5
    val iterations = 100
    val configuration = new Configuration

    // Get a plan builder.
    val wayangContext = new WayangContext(new Configuration)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Spark.basicPlugin)
    val planBuilder = new PlanBuilder(wayangContext)
      .withJobName(s"k-means ($inputUrl, k=$k, $iterations iterations)")
      .withUdfJarsOf(this.getClass)

    case class Point(x: Double, y: Double)
    case class TaggedPoint(x: Double, y: Double, cluster: Int)
    case class TaggedPointCounter(x: Double, y: Double, cluster: Int, count: Long) {
      def add_points(that: TaggedPointCounter) = TaggedPointCounter(this.x + that.x, this.y + that.y, this.cluster, this.count + that.count)
      def average = TaggedPointCounter(x / count, y / count, cluster, 0)
    }

    // Read and parse the input file(s).
    val points = planBuilder
      .readTextFile(inputUrl).withName("Read file")
      .map { line =>
        val fields = line.split(",")
        Point(fields(0).toDouble, fields(1).toDouble)
      }.withName("Create points")


    // Create initial centroids.
    val random = new Random
    val initialCentroids = planBuilder
      .loadCollection(for (i <- 1 to k) yield TaggedPointCounter(random.nextGaussian(), random.nextGaussian(), i, 0)).withName("Load random centroids")

    // Declare UDF to select centroid for each data point.
    class SelectNearestCentroid extends ExtendedSerializableFunction[Point, TaggedPointCounter] {

      /** Keeps the broadcasted centroids. */
      var centroids: Iterable[TaggedPointCounter] = _

      override def open(executionCtx: ExecutionContext) = {
        centroids = executionCtx.getBroadcast[TaggedPointCounter]("centroids")
      }

      override def apply(point: Point): TaggedPointCounter = {
        var minDistance = Double.PositiveInfinity
        var nearestCentroidId = -1
        for (centroid <- centroids) {
          val distance = Math.pow(Math.pow(point.x - centroid.x, 2) + Math.pow(point.y - centroid.y, 2), 0.5)
          if (distance < minDistance) {
            minDistance = distance
            nearestCentroidId = centroid.cluster
          }
        }
        new TaggedPointCounter(point.x, point.y, nearestCentroidId, 1)
      }
    }

    // Do the k-means loop.
    val finalCentroids = initialCentroids.repeat(iterations, { currentCentroids =>
      points
        .mapJava(new SelectNearestCentroid,
          udfLoad = LoadProfileEstimators.createFromSpecification(
            "my.udf.costfunction.key", configuration
          ))
        .withBroadcast(currentCentroids, "centroids").withName("Find nearest centroid")
        .reduceByKey(_.cluster, _.add_points(_)).withName("Add up points")
        .withCardinalityEstimator(k)
        .map(_.average).withName("Average points")
    }).withName("Loop")

      // Collect the results.
      .collect()

    println(finalCentroids)
  }
}
```

## License

All files in this repository are licensed under the Apache Software License 2.0

Copyright 2020 - 2026 The Apache Software Foundation.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
