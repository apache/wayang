<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
--->

# Apache Wayang Benchmarks <img align="right" width="128px" src="https://wayang.apache.org/img/wayang.png" alt="Wayang logo">

This repository provides example applications and further benchmarking tools to evaluate and get started with [Apache Wayang](https://wayang.apache.org).

Below we provide detailed information on our various benchmark components, including running instructions. For the configuration of Apache Wayang (incubating) itself, please consult the [Apache Wayang (incubating) repository](https://github.com/apache/incubator-wayang) or feel free to reach out on [dev@wayang.apache.org](mailto:dev@wayang.apache.org).


## Apache Wayang Example Applications

### Launching the main class

To run any of the following example applications, use this format:

```shell
<main class> exp(<ID>[,tags=<tag>,...][,conf=<key>:<value>,...]) <plugin>(,<plugin>)* <arg1> <arg2> ...
```

Replace `<arg1> <arg2>, ...` with the application-specific parameters that you want to use.

For example, to run the `org.apache.wayang.apps.wordcount.WordCountScala` class with the `exp(123)` experiment descriptor and the `java` plugin, use:
```bash
./bin/wayang-submit org.apache.wayang.apps.wordcount.WordCountScala exp\(123\) java file://$(pwd)/README.md
```

**Note** that the `file://$(pwd)/example_file.txt` format should be used for dealing with files.

### WordCount

**Description.** This app takes a text input file and counts the number occurrences of each word in the text. This simple app has become some sort of _"Hello World"_ program for data processing systems.

**Running the app.** To run the app, launch the main class:
```java
org.apache.wayang.apps.wordcount.WordCountScala
```
Even though this app is written in Scala, you can launch it in a regular JVM. Run the app without parameters to get a description of the required parameters.

**Datasets.** Find below a list of datasets that can be used to benchmark Apache Wayang (incubating) in combination with this app:
* [DBpedia - Long abstracts](http://wiki.dbpedia.org/Downloads2015-10) _NB: Consider stripping of the RDF container around the abstracts. It's not necessary, though._

### Word2NVec

**Description.** Akin to Google's [Word2Vec](https://arxiv.org/abs/1301.3781), this app creates vector representations of words from a corpus based on its neighbors. This app is a bit simpler in the sense that it calculates the average neighborhood of each word rather than determining a lower-dimensional representation. The resulting vectors can be used, e.g., to cluster words and find related terms.

**Running the app.** To run the app, launch the main class:
```java
org.apache.wayang.apps.simwords.Word2NVec
```
Even though this app is written in Scala, you can launch it in a regular JVM. Run the app without parameters to get a description of the required parameters.

**Datasets.** Find below a list of datasets that can be used to benchmark Apache Wayang (incubating) in combination with this app:
* [DBpedia - Long abstracts](http://wiki.dbpedia.org/Downloads2015-10) _NB: Consider stripping of the RDF container around the abstracts. It's not necessary, though._

### TPC-H Query 3

**Description.** This app executes a query from the established TPC-H benchmark. We provide several variants that work either on data in databases, in files, or in a mixture of both. Thus, this app requires cross-platform execution.

**Running the app.** To run the app, launch the main class:
```java
org.apache.wayang.apps.tpch.TpcH
```
Even though this app is written in Scala, you can launch it in a regular JVM. Run the app without parameters to get a description of the required parameters. Note that you will have to configure Apache Wayang (incubating), such that can access the database. Furthermore, this app depends on the following configuration keys:
* `wayang.apps.tpch.csv.customer`: URL to the `CUSTOMER` file
* `wayang.apps.tpch.csv.orders`: URL to the `ORDERS` file
* `wayang.apps.tpch.csv.lineitem`: URL to the `LINEITEM` file

**Datasets.** The datasets for this app can be generated with the [TPC-H tools](https://www.tpc.org/tpch/). The generated datasets can then be either put into a database and/or a filesystem.

### SINDY

**Description.** This app provides the data profiling algorithm [SINDY](https://subs.emis.de/LNI/Proceedings/Proceedings241/article24.html) that discovers inclusion dependencies in a relational database.

**Running the app.** To run the app, launch the main class:
```java
org.apache.wayang.apps.sindy.Sindy
```
Even though this app is written in Scala, you can launch it in a regular JVM. Run the app without parameters to get a description of the required parameters.

**Datasets.** Find below a list of datasets that can be used to benchmark Apache Wayang (incubating) in combination with this app:
* CSV files generated with the [TPC-H tools](https://www.tpc.org/tpch/)
* [other CSV files](https://hpi.de/naumann/projects/repeatability/data-profiling/metanome-ind-algorithms.html)

### SGD

**Description.** This app implements the stochastic gradient descent algorithm. SGD is an optimization algorithm that minimizes a loss function and can be used in many tasks of supervised machine learning. The current implementation uses the logistic loss and can thus, be used for classification. As many other machine learning techniques, SGD is a highly iterative algorithm.

**Running the app.** To run the app, launch the main class:
```java
org.apache.wayang.apps.sgd.SGD
```
Even though this app is written in Scala, you can launch it in a regular JVM. Run the app without parameters to get a description of the required parameters.

**Datasets.** Find below a list of datasets that can be used to benchmark Apache Wayang (incubating) in combination with this app:
* [HIGGS](https://archive.ics.uci.edu/ml/datasets/HIGGS)
* [Other datasets](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary.html)

### k-means

**Description.** Being a well-known method to cluster data points in a Euclidian space. As many other machine learning techniques, k-means is an iterative algorithm.

**Running the app.** To run the app, launch the main class:
```java
org.apache.wayang.apps.kmeans.Kmeans
```
or
```java
org.apache.wayang.apps.kmeans.postgres.Kmeans
```
The latter assumes data to reside in a filesystem, while the other assumes data to reside in PostgreSQL. For the latter case, you will have to configure Apache Wayang (incubating), such that it can access the database.
Even though this app is written in Scala, you can launch it in a regular JVM. Run the app without parameters to get a description of the required parameters.

**Datasets.** We provide a [data generator](https://github.com/apache/incubator-wayang/blob/main/wayang-benchmark/code/test/resources/kmeans-datagenerator.py) to generate files that can be clustered. You can further load these files into the database assuming the following schema:
```sql
CREATE TABLE "<table_name_of_your_choice>" (x float8, y float8);
```

### CrocoPR

**Description.** This app implements the cross-community PageRank: It takes as input two graphs, merges them, and runs a standard PageRank on the resulting graph. The preprocessing and PageRank steps typically lend themselves to be executed on different platforms.

**Running the app.** To run the app, launch the main class:
```java
org.apache.wayang.apps.crocopr.CrocoPR
```
Even though this app is written in Scala, you can launch it in a regular JVM. Run the app without parameters to get a description of the required parameters.

**Datasets.** This app works on RDF files, more specifically the [Wikipedia pagelinks via DBpedia](http://wiki.dbpedia.org/Downloads2015-10). Note that this app requires two input files. For the purpose of benchmarking, it is fine to use the same input file twice.

## Optimizer experiments

### Optimizer scalability

**Description.** This app generates Apache Wayang (incubating) plans with specific predefined topologies but of arbitrary size. This allows to experimentally determine the scalability of Apache Wayang (incubating)'s optimizer to large plans.

**Running the app.** To run the app, launch the main class:
```java
org.apache.wayang.apps.benchmark.OptimizerScalabilityTest
```
Even though this app is written in Scala, you can launch it in a regular JVM. Run the app without parameters to get a description of the required parameters. Furthermore, the following configuration can be interesting:
- `wayang.core.optimizer.pruning.strategies`: controls the pruning strategy to be used when enumerating alternative plans
  - admissible values: empty or comma-separated list of `org.apache.wayang.core.optimizer.enumeration.LatentOperatorPruningStrategy` (default), `org.apache.wayang.core.optimizer.enumeration.TopKPruningStrategy`, `org.apache.wayang.core.optimizer.enumeration.RandomPruningStrategy`, and `org.apache.wayang.core.optimizer.enumeration.SinglePlatformPruningStrategy` (order-sensitive)
- `wayang.core.optimizer.pruning.topk`: controls the _k_ for the top-k pruning
- `wayang.core.optimizer.enumeration.concatenationprio`: controls the order of the enumeration
  - admissible values: `slots`, `plans`, `plans2`, `none`, `random`
- `wayang.core.optimizer.enumeration.invertconcatenations` invert the above mentioned enumeration order
  - admissible value: `false` (default), `true`
  
