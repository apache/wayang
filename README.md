<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Apache Wayang™ <img align="right" width="128px" src="https://wayang.apache.org/img/wayang.png" alt="Wayang Logo">

## The first open-source cross-platform data processing system

**Write your data pipeline once. Run it anywhere.**

[![Maven central](https://img.shields.io/maven-central/v/org.apache.wayang/wayang-core.svg?style=for-the-badge)](https://central.sonatype.com/artifact/org.apache.wayang/wayang-core)
[![License](https://img.shields.io/github/license/apache/wayang.svg?style=for-the-badge)](http://www.apache.org/licenses/LICENSE-2.0)
[![Last commit](https://img.shields.io/github/last-commit/apache/wayang.svg?style=for-the-badge)]()
![GitHub commit activity (branch)](https://img.shields.io/github/commit-activity/m/apache/wayang?style=for-the-badge)
![GitHub forks](https://img.shields.io/github/forks/apache/wayang?style=for-the-badge)
![GitHub Repo stars](https://img.shields.io/github/stars/apache/wayang?style=for-the-badge)

[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Apache%20Wayang%20enables%20cross%20platform%20data%20processing,%20star%20it%20via:%20&url=https://github.com/apache/wayang&via=apachewayang&hashtags=dataprocessing,bigdata,analytics,hybridcloud,developers) [![LinkedIn](https://img.shields.io/badge/LinkedIn-Follow-0A66C2?style=social&logo=linkedin)](https://www.linkedin.com/company/apachewayang)

You write your pipeline against a single API, then decide how it runs. Point it at one engine and it runs there. Or hand Wayang's cost-based optimizer the choice and let it pick the best platform for each step across your laptop, Apache Spark, Apache Flink, or a database, even splitting a single job across several. Either way, when your data outgrows one machine you don't rewrite anything, you just make another engine available.

<p align="center">
  <img src="guides/img/wayang-architecture.svg" alt="A single pipeline, written once, feeds the Wayang optimizer, which routes each step to the best available engine — Local, Spark, Flink, Postgres, and others." width="720" />
</p>

## Table of contents

- [How it works](#how-it-works)
- [Quickstart](#quickstart)
- [Install](#install)
- [Documentation](#documentation)
- [Research](#research)
- [Contributing](#contributing)
- [Community](#community)
- [Authors](#authors)
- [License](#license)
- [Acknowledgements](#acknowledgements)

## How it works

Most data processing systems are designed around a single execution engine. That keeps things simple, but your pipeline ends up tied to that engine's API. So combining engines, or moving to another, typically means rewriting and gluing together which is costly and time-consuming.

Wayang sits one level up. You write a pipeline against Wayang's API and register the engines you *have*. Then it's your call. Want control? Register one engine and it runs there. Want it handled? Register several and let the cost-based optimizer pick the best one for each step, even splitting a single job across engines.

**Supported platforms today**

- [Java Streams](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html)
- [Apache Spark](https://spark.apache.org/)
- [Apache Flink](https://flink.apache.org/)
- [Apache Giraph](https://giraph.apache.org/)
- [PostgreSQL](http://www.postgresql.org)
- [SQLite](https://www.sqlite.org/)
- [Apache Kafka](https://kafka.apache.org)
- [TensorFlow](https://www.tensorflow.org/)

**Wayang's APIs**

- Java (Scala-like fluent builder)
- Scala
- SQL
- Java native (low-level, we recommend the fluent scala-like)

The plugin architecture makes adding new operators and platforms straightforward without touching internals — see [Adding operators](https://wayang.apache.org/docs/guide/adding-operators).

## Quickstart

We'll run a word count locally first — no cluster, nothing to install on a server — then make Spark available with a one-line change. The pipeline itself never changes; only the set of engines you register does.

### 1. Run locally

```java
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.java.Java;
import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        // Register ONLY the local Java engine → runs on your machine, no cluster needed.
        WayangContext wayang = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin());

        new JavaPlanBuilder(wayang)
                .withJobName("WordCount")
                .withUdfJarOf(WordCount.class)
                .readTextFile("file:///path/to/input.txt")
                .flatMap(line -> Arrays.asList(line.split("\\W+")))
                .filter(word -> !word.isEmpty())
                .map(word -> new Tuple2<>(word.toLowerCase(), 1))
                .reduceByKey(Tuple2::getField0,
                             (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1()))
                .writeTextFile("file:///path/to/output.txt", t -> t.getField0() + ": " + t.getField1());
    }
}
```

It executes locally. Good for development, tests, and small data.

### 2. Run it on Spark

Now run the *exact same pipeline* on Spark instead of locally. You don't touch the pipeline — you change which platform you register: comment out Java and register Spark.

```java
import org.apache.wayang.spark.Spark;               // swap the import

// Same pipeline as before — only the registered platform changed.
WayangContext wayang = new WayangContext(new Configuration())
        // .withPlugin(Java.basicPlugin())           // comment out the local engine
        .withPlugin(Spark.basicPlugin());            // register Spark instead
```

Run it again. The same pipeline now executes on Spark. You changed *where* it runs without changing *what* it does. Switch to Flink or any other supported platform the same way: swap the import and the registered plugin.

> **Why register only Spark here?** Wayang's real power is registering several platforms and letting the optimizer pick. But on small test data the optimizer will almost always pick the local engine (Spark's startup overhead isn't worth it for a tiny file) so you'd never actually see Spark run. Registering Spark alone forces the issue so you can confirm it works. Step 3 shows the production pattern.

### 3. Register both and let the optimizer choose

This is the point of Wayang. In practice you don't pick a platform at all: you register every engine you have and let the optimizer choose the best one for each step.

```java
// Register BOTH platforms — Wayang's optimizer decides which to use per step.
WayangContext wayang = new WayangContext(new Configuration())
        .withPlugin(Java.basicPlugin())
        .withPlugin(Spark.basicPlugin());
```

Now Wayang owns the placement decision. For each operator it estimates the cost on every registered platform and picks the cheapest, keeping a small job entirely local, pushing a large one onto Spark, or mixing both within the same job as the data and query demands. On a tiny input you'll see it keep everything local (that's the optimizer working correctly, not ignoring Spark); cross-platform splits show up once the data is big enough to justify them.

## Install

Replace `WAYANG_VERSION` with the [latest Maven Central release](https://central.sonatype.com/artifact/org.apache.wayang/wayang-core).

### From Maven Central

```xml
<dependency>
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-core</artifactId>
  <version>WAYANG_VERSION</version>
</dependency>
<dependency>
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-basic</artifactId>
  <version>WAYANG_VERSION</version>
</dependency>
<dependency>
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-api-scala-java</artifactId>
  <version>WAYANG_VERSION</version>
</dependency>
<!-- add one artifact per engine you want available -->
<dependency>
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-java</artifactId>
  <version>WAYANG_VERSION</version>
</dependency>
<dependency>
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-spark</artifactId>
  <version>WAYANG_VERSION</version>
</dependency>
```

The available modules:

- `wayang-core` — core data structures and the optimizer (**required**)
- `wayang-basic` — common operators and data types (recommended)
- `wayang-api-scala-java` — fluent Scala/Java API for building plans (recommended)
- `wayang-java`, `wayang-spark`, `wayang-flink`, `wayang-postgres`, `wayang-sqlite3`, `wayang-giraph`, `wayang-tensorflow` — per-platform adapters; include one per engine you want available
- `wayang-profiler` — learns operator and UDF cost functions from historical executions

For snapshot builds, add Apache's snapshot repository:

```xml
<repositories>
  <repository>
    <id>apache-snapshots</id>
    <name>Apache Foundation Snapshot Repository</name>
    <url>https://repository.apache.org/content/repositories/snapshots</url>
  </repository>
</repositories>
```

### Build from source

```bash
git clone https://github.com/apache/wayang.git
cd wayang
./mvnw clean install -DskipTests
```

The current snapshot version lives in [`pom.xml`](https://github.com/apache/wayang/blob/main/pom.xml).

### Runtime requirements

- **Java 17** — set `JAVA_HOME` to your Java 17 installation.
- **Apache Spark 3.4.4** with Scala 2.12 — set `SPARK_HOME`.
- **Apache Hadoop 3+** — set `HADOOP_HOME`.
- **Maven** for building from source.

> [!IMPORTANT]
> **Java 17 needs extra JVM flags.** Running Wayang on Java 17 (especially with Spark) requires opening some internal Java modules, or you'll hit `IllegalAccessError`. Edit your `wayang-submit` script (under `wayang-assembly/target/wayang-WAYANG_VERSION/bin/wayang-submit`) so the runner invocation passes:
>
> ```
> --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
> --add-opens=java.base/java.nio=ALL-UNNAMED
> --add-opens=java.base/java.lang=ALL-UNNAMED
> --add-opens=java.base/java.util=ALL-UNNAMED
> --add-opens=java.base/java.io=ALL-UNNAMED
> --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
> --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
> --add-opens=java.base/java.net=ALL-UNNAMED
> --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
> ```
>
> On Windows, also set `HADOOP_HOME` to a directory containing `winutils.exe` ([unofficial source](https://github.com/steveloughran/winutils)).

### Validate the install

After building, unpack the assembly and put Wayang on your `PATH`:

```bash
tar -xvf wayang-WAYANG_VERSION.tar.gz
cd wayang-WAYANG_VERSION

# Linux
echo "export WAYANG_HOME=$(pwd)" >> ~/.bashrc
echo "export PATH=${PATH}:${WAYANG_HOME}/bin" >> ~/.bashrc
source ~/.bashrc

# macOS
echo "export WAYANG_HOME=$(pwd)" >> ~/.zshrc
echo "export PATH=${PATH}:${WAYANG_HOME}/bin" >> ~/.zshrc
source ~/.zshrc
```

Then run the bundled WordCount on your local Java engine:

```bash
bin/wayang-submit org.apache.wayang.apps.wordcount.Main java file://$(pwd)/README.md
```

### Running the tests

```bash
./mvnw test
```

## Documentation

- **[Getting started](https://wayang.apache.org/docs/guide/getting-started)** — the full tabbed walkthrough in Java, Scala, and Python.
- **[How Wayang chooses a platform](https://wayang.apache.org/docs/introduction/about)** — what drives the optimizer's decisions.
- **[Adding operators](https://wayang.apache.org/docs/guide/adding-operators)** — extend Wayang with new operators or platforms.
- **[Example applications](guides/wayang-examples.md)** — runnable apps in this repo.
- **[Developing with Wayang](guides/develop-with-Wayang.md)** — using Wayang in your own Java/Scala project.

## Research

Wayang (formerly called Rheem) is the product of many years of top quality research. Below you can find the main publications:

- Apache Wayang: A Unified Data Analytics Framework. SIGMOD Rec. 52(3): 30-35 (2023) [pdf](https://sigmodrecord.org/publications/sigmodRecord/2309/pdfs/05_Systems_Beedkar.pdf)
- Apache Wayang in Action: Enabling Data Systems Integration via a Unified Data Analytics Framework. SIGMOD Conference Companion 2025: 35-38 [pdf](https://dl.acm.org/doi/pdf/10.1145/3722212.3725081)
- ML-based Cross-Platform Query Optimization. ICDE 2020: 1489-1500 [pdf](https://itu.dk/~joqu/assets/publications/icde20.pdf)
- RHEEMix in the data jungle: a cost-based optimizer for cross-platform systems. VLDB J. 29(6): 1287-1310 (2020) [pdf](https://link.springer.com/article/10.1007/s00778-020-00612-x)
- RHEEM: Enabling Cross-Platform Data Processing - May The Big Data Be With You! -. Proc. VLDB Endow. 11(11): 1414-1427 (2018) [pdf](http://www.vldb.org/pvldb/vol11/p1414-agrawal.pdf)

### Citing Wayang

If you use Apache Wayang in your research, please cite the SIGMOD Record paper:

```bibtex
@article{beedkar2023wayang,
  author  = {Kaustubh Beedkar and Bertty Contreras-Rojas and Haralampos Gavriilidis and Zoi Kaoudi and Volker Markl and Rodrigo Pardo-Meza and Jorge-Arnulfo Quian{\'{e}}-Ruiz},
  title   = "{Apache Wayang: A Unified Data Analytics Framework}",
  journal = {{SIGMOD} Rec.},
  volume  = {52},
  number  = {3},
  pages   = {30--35},
  year    = {2023},
  doi     = {10.1145/3631504.3631510}
}
```


## Contributing

Contributions are welcome — bug reports, doc fixes, new platform adapters, new operators, optimizer improvements, anything. Start with [CONTRIBUTING.md](CONTRIBUTING.md) and the [building guide](guides/develop-in-Wayang.md), open an issue if you're not sure where to start, and introduce yourself on the [dev mailing list](https://wayang.apache.org/docs/community/mailinglist) — that's where active work gets discussed.

If you're looking for somewhere to begin, doc improvements, new operators, and additional examples are areas where a focused PR can land quickly.

## Community

- **Mailing lists** — [https://wayang.apache.org/docs/community/mailinglist](https://wayang.apache.org/docs/community/mailinglist) (user and dev)
- **LinkedIn** — [Apache Wayang](https://www.linkedin.com/company/apachewayang)

## Authors

See the full list of [contributors](https://github.com/apache/wayang/graphs/contributors).

## License

All files in this repository are licensed under the Apache License 2.0.

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

## Acknowledgements

The [logo](https://wayang.apache.org/img/wayang.png) was donated by Brian Vera.
