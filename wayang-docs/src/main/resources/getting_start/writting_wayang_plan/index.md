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
title: "Wayang Abstractions & Plans"
previous:
    url: /getting_start/how_run/
    title: How to Run Wayang
next:
    url: /using_wayang/
    title: Using Wayang
menu:
    getting_start:
        weight: 3
---

# Wayang Abstractions & Writing Plans

Apache Wayang represents data processing applications as directed acyclic (or cyclic) dataflow graphs composed of platform-independent logical operators. At optimization time, Wayang translates these high-level operators into concrete execution operators mapped to the optimal underlying processing platforms (such as Java Streams, Apache Spark, Flink, or relational databases).

---

## Core Operator Abstractions

Wayang categorizes all data transformations into five fundamental operator archetypes:

### 1. Source Operators
Source operators serve as the root entry points of a Wayang plan. They ingest raw data from external storage systems or collections without accepting input channels:
- **`TextFileSource`**: Reads text files from local disk, HDFS, or S3 line by line.
- **`TableSource`**: Queries structured relational database tables or views (e.g., PostgreSQL, SQLite).
- **`CollectionSource`**: Wraps in-memory Java/Scala collections into a distributed dataflow.

### 2. Unary Operators
Unary operators process a single input dataset to produce a transformed output dataset:
- **`MapOperator`**: Applies a transformation function to each element (1-to-1).
- **`FilterOperator`**: Retains elements satisfying a boolean predicate.
- **`FlatMapOperator`**: Transforms each element into zero, one, or more output elements.
- **`ReduceByOperator`**: Aggregates elements sharing the same key.
- **`SortOperator`**: Orders records by specific sort keys.
- **`CountOperator`**: Calculates dataset cardinality.

### 3. Binary Operators
Binary operators accept two distinct input datasets and produce an output dataset:
- **`JoinOperator`**: Performs relational inner or outer joins matching key extractors across two datasets.
- **`UnionAllOperator`**: Combines two datasets of identical data types into one unified stream.
- **`CartesianOperator`**: Computes the full cross-product of two input collections.
- **`IntersectOperator`**: Returns the set intersection between two collections.

### 4. Loop Operators
Loop operators support iterative and recursive processing cycles, enabling complex graph and machine learning workflows:
- **`LoopOperator` / `DoWhileOperator`**: Iteratively applies a loop body until a convergence predicate or maximum iteration count is reached (crucial for algorithms like PageRank, K-Means, and Gradient Descent).
- **`RepeatOperator`**: Repeats a sub-plan for a fixed number of iterations.

### 5. Sink Operators
Sink operators terminate the execution plan by writing output datasets to destination sinks or returning them to the driver application:
- **`TextFileSink`**: Serializes records to text files on local storage or distributed filesystems.
- **`CollectionSink` / `LocalCallbackSink`**: Collects records back into JVM memory or invokes a user callback for each produced element.

---

## Assembling Plans with `PlanBuilder`

The primary way to author Wayang applications in Java and Scala is through the fluent `PlanBuilder` API (`JavaPlanBuilder`).

### Key PlanBuilder Components
- **`WayangContext`**: Holds execution configuration and registers the available platform plugins (e.g., `Java.basicPlugin()`, `Spark.basicPlugin()`).
- **`JavaPlanBuilder`**: Provides fluent factory methods to chain operators together.
- **`DataQuantaBuilder`**: Represents a intermediate dataset within the plan, providing transformation methods (`map`, `filter`, `reduceByKey`, `join`).

### Example: Building a WordCount Plan

```java
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import java.util.Collection;
import java.util.Arrays;

public class WordCountExample {

    public static void main(String[] args) {
        // 1. Create a Wayang context with target plugins
        WayangContext context = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin());

        // 2. Initialize the fluent plan builder
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context);

        // 3. Assemble the plan: Source -> Unary -> Binary -> Sink
        Collection<Tuple2<String, Integer>> wordCounts = planBuilder
                .readTextFile("file:///path/to/input.txt")
                .flatMap(line -> Arrays.asList(line.toLowerCase().split("\\W+")))
                .withName("Split words")
                .filter(word -> !word.isEmpty())
                .withName("Filter empty")
                .map(word -> new Tuple2<>(word, 1))
                .withName("Pair with 1")
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withName("Aggregate counts")
                .collect();

        // 4. Output results
        wordCounts.forEach(t -> System.out.println(t.getField0() + ": " + t.getField1()));
    }
}
```

### Execution Lifecycle
When `.collect()` or a sink action is invoked:
1. Wayang analyzes the logical plan graph.
2. The optimizer assigns cost estimates to candidate platform operators.
3. Wayang selects the cheapest cross-platform execution plan.
4. Intermediate channels and data conversions are automatically inserted.
5. The plan is executed across the selected platforms.
