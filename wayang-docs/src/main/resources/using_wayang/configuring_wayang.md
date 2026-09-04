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
title: "Configuring Wayang"
previous:
    url: /using_wayang/
    title: Using Wayang
next:
    url: /using_wayang/cost_model_calibration/
    title: Cost Model Calibration
menu:
    using:
        weight: 5
---

# Configuring Apache Wayang

To enable Apache Wayang's smooth operation and intelligent optimization, you need to provide details about your processing platforms' capabilities, resources, and connection properties. 

While a default configuration is loaded automatically for local experimentation, creating a custom configuration properties file is recommended for fine-tuning performance or connecting to distributed execution engines.

---

## Loading Custom Configurations

You can load a custom configuration file into your application via the command-line JVM system property:

```shell
$ java -Dwayang.configuration=file:///path/to/my/wayang.properties -cp ... my.app.Main
```

Alternatively, you can load or modify configurations programmatically in your Java or Scala application:

```java
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;

Configuration config = new Configuration("file:///path/to/my/wayang.properties");
config.setProperty("wayang.spark.master", "spark://my-cluster:7077");

WayangContext wayangContext = new WayangContext(config);
```

---

## Key Configuration Properties

### General Core Settings
| Property | Default | Description |
|---|---|---|
| `wayang.core.log.enabled` | `false` | Whether to log execution statistics to allow learning better cardinality and cost estimators for the optimizer. |
| `wayang.core.log.executions` | `~/.wayang/executions.json` | Destination path where execution times of operator groups are recorded. |
| `wayang.core.log.cardinalities` | `~/.wayang/cardinalities.json` | Destination path where cardinality measurements are stored. |
| `wayang.core.optimizer.instrumentation` | `OutboundInstrumentationStrategy` | Strategy for measuring intermediate cardinalities (`NoInstrumentationStrategy`, `OutboundInstrumentationStrategy`, or `FullInstrumentationStrategy`). |
| `wayang.core.optimizer.reoptimize` | `false` | Whether to progressively re-optimize execution plans at runtime based on actual intermediate cardinalities. |
| `wayang.basic.tempdir` | `file:///tmp` | Location used for storing temporary files, especially for inter-platform data exchanges. |

---

### Java Streams Platform
| Property | Default | Description |
|---|---|---|
| `wayang.java.cpu.mhz` | `2700` | Clock frequency (MHz) of the processor executing the local JVM. |
| `wayang.java.hdfs.ms-per-mb` | `2.7` | Average throughput from HDFS to the local JVM in milliseconds per megabyte. |

---

### Apache Spark Platform
| Property | Default | Description |
|---|---|---|
| `spark.master` | `local` | Spark master URL (e.g., `local[*]`, `spark://host:port`, or `yarn`). |
| `spark.app.name` | `Wayang App` | Spark application name. |
| `wayang.spark.cpu.mhz` | `2700` | CPU clock frequency (MHz) of the Spark worker nodes. |
| `wayang.spark.hdfs.ms-per-mb` | `2.7` | Throughput from HDFS to Spark workers (ms/MB). |
| `wayang.spark.network.ms-per-mb` | `8.6` | Average network throughput between Spark workers (ms/MB). |
| `wayang.spark.init.ms` | `4500` | Overhead time (ms) required for Spark context initialization. |

---

### Relational Database Platforms (JDBC)

#### PostgreSQL
| Property | Description |
|---|---|
| `wayang.postgres.jdbc.url` | JDBC connection URL (e.g., `jdbc:postgresql://localhost:5432/mydb`). |
| `wayang.postgres.jdbc.user` | Database user account name. |
| `wayang.postgres.jdbc.password` | Database password. |
| `wayang.postgres.cpu.mhz` | Clock frequency (MHz) of the PostgreSQL database server. |
| `wayang.postgres.cpu.cores` | Number of CPU cores available on the PostgreSQL database server. |

#### SQLite3
| Property | Description |
|---|---|
| `wayang.sqlite3.jdbc.url` | JDBC connection URL (e.g., `jdbc:sqlite:/path/to/database.db`). |
| `wayang.sqlite3.cpu.mhz` | Clock frequency (MHz) of the processor running SQLite. |
| `wayang.sqlite3.cpu.cores` | Available CPU cores on the SQLite host machine. |

---

### Next Steps

For advanced cost-based optimization and calibrating load profile estimator templates with historical workload metrics, see [Cost Model Calibration]({% link using_wayang/cost_model_calibration.md %}).
