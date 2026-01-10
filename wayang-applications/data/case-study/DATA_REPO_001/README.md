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

# Apache Wayang <img align="right" width="128px" src="https://wayang.apache.org/img/wayang.png" alt="Wayang Logo">

## The first open-source cross-platform data processing system

[![Maven central](https://img.shields.io/maven-central/v/org.apache.wayang/wayang-core.svg?style=for-the-badge)](https://img.shields.io/maven-central/v/org.apache.wayang/wayang-core.svg)
[![License](https://img.shields.io/github/license/apache/incubator-wayang.svg?style=for-the-badge)](http://www.apache.org/licenses/LICENSE-2.0)
[![Last commit](https://img.shields.io/github/last-commit/apache/incubator-wayang.svg?style=for-the-badge)]()
![GitHub commit activity (branch)](https://img.shields.io/github/commit-activity/m/apache/incubator-wayang?style=for-the-badge)
![GitHub forks](https://img.shields.io/github/forks/apache/incubator-wayang?style=for-the-badge)
![GitHub Repo stars](https://img.shields.io/github/stars/apache/incubator-wayang?style=for-the-badge)

[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Apache%20Wayang%20enables%20cross%20platform%20data%20processing,%20star%20it%20via:%20&url=https://github.com/apache/incubator-wayang&via=apachewayang&hashtags=dataprocessing,bigdata,analytics,hybridcloud,developers) [![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/ApacheWayang?style=social)](https://www.reddit.com/r/ApacheWayang/)
## Table of contents
  * [Description](#description)
  * [Quick Guide for Running Wayang](#quick-guide-for-running-wayang)
  * [Quick Guide for Developing with Wayang](#quick-guide-for-developing-with-wayang)
  * [Installing Wayang](#installing-wayang)
    + [Requirements at Runtime](#requirements-at-runtime)
    + [Validating the installation](#validating-the-installation)
  * [Getting Started](#getting-started)
    + [Prerequisites](#prerequisites)
    + [Building](#building)
  * [Running the tests](#running-the-tests)
  * [Example Applications](#example-applications)
  * [Built With](#built-with)
  * [Contributing](#contributing)
  * [Authors](#authors)
  * [License](#license)

## Description

In contrast to traditional data processing systems that provide one dedicated execution engine, Apache Wayang can transparently and seamlessly integrate multiple execution engines and use them to perform a single task. We call this *cross-platform data processing*. In Wayang, users can specify any data processing application using one of Wayang's APIs and then Wayang will choose the data processing platform(s), e.g., Postgres or Apache Spark, that best fits the application. Finally, Wayang will perform the execution, thereby hiding the different platform-specific APIs and coordinating inter-platform communication.

Apache Wayang aims at freeing data engineers and software developers from the burden of learning all different data processing systems, their APIs, strengths and weaknesses; the intricacies of coordinating and integrating different processing platforms; and the inflexibility when trying a fixed set of processing platforms. As of now, Wayang has built-in support for the following processing platforms:
- [Java Streams](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html)
- [Apache Spark](https://spark.apache.org/)
- [Apache Flink](https://flink.apache.org/)
- [Apache Giraph](https://giraph.apache.org/)
- [GraphChi](https://github.com/GraphChi/graphchi-java)
- [Postgres](http://www.postgresql.org)
- [SQLite](https://www.sqlite.org/)

Apache Wayang can be used via the following APIs:
- Java native
- Java scala-like
- Scala
- SQL (limited support of simple select-project queries for now)

## Quick Guide for Running Wayang

For a quick guide on how to run WordCount see [here](guides/tutorial.md).

## Quick Guide for Developing with Wayang

For a quick guide on how to use Wayang in your Java/Scala project see [here](guides/develop-with-Wayang.md).

## Installing Wayang

You first have to build the binaries as shown [here](guides/tutorial.md).
Once you have the binaries built, follow these steps to install Wayang:

```shell
tar -xvf wayang-0.7.1-snapshot.tar.gz
cd wayang-0.7.1-SNAPSHOT
```

In linux
```shell
echo "export WAYANG_HOME=$(pwd)" >> ~/.bashrc
echo "export PATH=${PATH}:${WAYANG_HOME}/bin" >> ~/.bashrc
source ~/.bashrc
```
In MacOS
```shell
echo "export WAYANG_HOME=$(pwd)" >> ~/.zshrc
echo "export PATH=${PATH}:${WAYANG_HOME}/bin" >> ~/.zshrc
source ~/.zshrc
```

### Requirements at Runtime

Since Apache Wayang is not an execution engine itself but rather manages the execution engines for you, it is important to have the necessary requirements installed.

- Apache Wayang supports Java versions 8 and above. However, the Wayang team recommends using Java version 11. Don’t forget to set the `JAVA_HOME` environment variable.
- You need to install Apache Spark version 3 or higher. Don’t forget to set the `SPARK_HOME` environment variable.
- You need to install Apache Hadoop version 3 or higher. Don’t forget to set the `HADOOP_HOME` environment variable.

### Validating the installation

To execute your first application with Apache Wayang, you need to execute your program with the 'wayang-submit' command:

```shell
bin/wayang-submit org.apache.wayang.apps.wordcount.Main java file://$(pwd)/README.md
```

## Getting Started

Wayang is available via Maven Central. To use it with Maven, include the following code snippet into your POM file:
```xml
<dependency>
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-***</artifactId>
  <version>0.7.1</version>
</dependency>
```
Note the `***`: Wayang ships with multiple modules that can be included in your app, depending on how you want to use it:
* `wayang-core`: provides core data structures and the optimizer (required)
* `wayang-basic`: provides common operators and data types for your apps (recommended)
* `wayang-api-scala-java`: provides an easy-to-use Scala and Java API to assemble Wayang plans (recommended)
* `wayang-java`, `wayang-spark`, `wayang-graphchi`, `wayang-sqlite3`, `wayang-postgres`: adapters for the various supported processing platforms
* `wayang-profiler`: provides functionality to learn operator and UDF cost functions from historical execution data

> **NOTE:** The module `wayang-api-scala-java` is intended to be used with Java 11 and Scala 2.12.

For the sake of version flexibility, you still have to include in the POM file your Hadoop (`hadoop-hdfs` and `hadoop-common`) and Spark (`spark-core` and `spark-graphx`) version of choice.

In addition, you can obtain the most recent snapshot version of Wayang via Sonatype's snapshot repository. Just include:
```xml
<repositories>
  <repository>
    <id>apache-snapshots</id>
    <name>Apache Foundation Snapshot Repository</name>
    <url>https://repository.apache.org/content/repositories/snapshots</url>
  </repository>
</repositories>
```

### Prerequisites
Apache Wayang is built with Java 11 and Scala 2.12. However, to run Apache Wayang it is sufficient to have just Java 11 installed. Please also consider that processing platforms employed by Wayang might have further requirements.
```
Java 11
[Scala 2.12]
```

> **NOTE:** In windows, you need to define the variable `HADOOP_HOME` with the winutils.exe, an not official option to obtain [this repository](https://github.com/steveloughran/winutils), or you can generate your winutils.exe following the instructions in the repository. Also, you may need to install [msvcr100.dll](https://www.microsoft.com/en-us/download/details.aspx?id=26999)

> **NOTE:** Make sure that the JAVA_HOME environment variable is set correctly to Java 11 as the prerequisite checker script currently supports up to Java 11 and checks the latest version of Java if you have higher version installed. In Linux, it is preferably to use the export JAVA_HOME method inside the project folder. It is also recommended running './mvnw clean install' before opening the project using IntelliJ.


### Building

If you need to rebuild Wayang, e.g., to use a different Scala version, you can simply do so via Maven:

1. Adapt the version variables (e.g., `spark.version`) in the main `pom.xml` file.
2. Build Wayang with the adapted versions.
    ```shell
   git clone https://github.com/apache/incubator-wayang.git
   cd incubator-wayang
   ./mvnw clean install -DskipTests
    ```
> **NOTE:** If you receive an error about not finding `MathExBaseVisitor`, then the problem might be that you are trying to build from IntelliJ, without Maven. MathExBaseVisitor is generated code, and a Maven build should generate it automatically.

> **NOTE:** In the current Maven setup, the version of scala is tied to the Java version, you can compile the profile `scala-11` with Java 8 and profile `scala-12` with Java 11.

> **NOTE:** For compiling and testing the code it is required to have Hadoop installed on your machine.

> **NOTE:**  the `standalone` profile to fix Hadoop and Spark versions, so that Wayang apps do not explicitly need to declare the corresponding dependencies.
>
> Also, note the `distro` profile, which assembles a binary Wayang distribution.
To activate these profiles, you need to specify them when running maven, i.e.,

```shell
./mvnw clean install -DskipTests -P<profile name>
```

## Running the tests
In the incubator-wayang root folder run:
```shell
./mvnw test
```

## Example Applications
You can see examples on how to start using Wayang [here](guides/wayang-examples.md)

## Built With

* [Java 11](https://www.oracle.com/de/java/technologies/javase/jdk11-archive-downloads.html)
* [Scala 2.12](https://www.scala-lang.org/download/2.12.0.html)
* [Maven](https://maven.apache.org/)

## Contributing
Before submitting a PR, please take a look on how to contribute with Apache Wayang contributing guidelines [here](CONTRIBUTING.md).

There is also a guide on how to compile your code [here](guides/develop-in-Wayang.md).
## Authors
The list of [contributors](https://github.com/apache/incubator-wayang/graphs/contributors).

## License
All files in this repository are licensed under the Apache Software License 2.0

Copyright 2020 - 2024 The Apache Software Foundation.

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
The [Logo](http://wayang.apache.org/assets/img/logo/Apache_Wayang/Apache_Wayang.pdf) was donated by Brian Vera.
