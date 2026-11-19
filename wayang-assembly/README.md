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

# Wayang Assembly
This is an assembly module for Apache Wayang project.

It creates a single tar.gz file that includes all needed dependency of the project
except for the jars in the list

- org.apache.hadoop.*, those are supposed to be available from the deployed Hadoop cluster.

> Note: This module is off by default. To activate it specify the profile in the command line
-Pdistribution

> Note: If you need to build an assembly for a different version of Hadoop the
> hadoop-version system property needs to be set as in this example: `-Dhadoop.version=2.7.4` at the 
> maven command line


# Execution Profile Assembly

To execute the Wayang Assembly you need to execute the following command in the project root

```shell
./mvnw clean install -DskipTests 
./mvnw clean package -pl :wayang-assembly -Pdistribution
```

# Docker Image

The project Docker image packages the Wayang assembly with a Java 17 runtime. It
does not distribute external execution platforms such as Apache Spark, Apache
Hadoop, Apache Flink, or database drivers. Those runtimes stay outside the image
and can be connected at container runtime.

Build the assembly first, then build the image from the project root. Do not use
the Maven `standalone` profile for this image, because that profile changes
external platform dependencies from `provided` to `compile`.

```shell
./mvnw clean install -Dmaven.test.skip=true
./mvnw package -pl :wayang-assembly -Pdistribution -Dmaven.test.skip=true
docker build -t apache-wayang:local .
```

Run the default Java platform smoke test:

```shell
docker run --rm apache-wayang:local
```

Run a specific Wayang application by passing the main class and its arguments to
`wayang-submit`:

```shell
docker run --rm apache-wayang:local \
  org.apache.wayang.apps.pi.PiEstimation java 1
```

To connect external platforms, mount or install their runtimes separately and
set the corresponding environment variables:

```shell
docker run --rm \
  -v /path/to/spark:/opt/spark:ro \
  -v /path/to/hadoop:/opt/hadoop:ro \
  -e SPARK_HOME=/opt/spark \
  -e HADOOP_HOME=/opt/hadoop \
  apache-wayang:local \
  org.apache.wayang.apps.pi.PiEstimation spark 1
```

Additional platform libraries, such as JDBC drivers, can be supplied with
`WAYANG_EXTRA_CLASSPATH`. A custom Wayang configuration can be mounted at
`${WAYANG_HOME}/conf/wayang.properties` and activated with `FLAG_WAYANG=true`.
Logging is configured with Log4j 2: set `FLAG_LOG=true` to load
`${WAYANG_HOME}/conf/log4j2.properties`, which can be edited or replaced by a mounted file.
Applications still need to register the platform plugins they use.
