<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# DuckDB Cost Calibration

Generate execution logs using `DuckDBCostPilotIT` as described in the
[platform README](../wayang-platforms/wayang-duckdb/README.md#cost-profiling).
The pilot remains a platform test; the genetic optimizer is provided by this
module.

From the repository root, install the profiler with the optional DuckDB runtime
(the profile makes DuckDB operators available when reading execution logs):

```sh
./mvnw -Pskip-prerequisite-check,duckdb -pl wayang-profiler -am -DskipTests -Dpython.worker.tests.skip=true install
```

Run the existing optimizer with the calibration configuration and execution log:

```sh
./mvnw -Pskip-prerequisite-check,duckdb -pl wayang-profiler exec:java -Dexec.mainClass=org.apache.wayang.profiler.log.GeneticOptimizerApp "-Dexec.args=file:///absolute/path/to/wayang/wayang-profiler/src/main/resources/duckdb-ga.properties wayang-platforms/wayang-duckdb/target/cost-profiling/duckdb/executions.json"
```

On Windows, replace `./mvnw` with `.\mvnw.cmd`. Paths are relative to the
repository root. Replace the configuration URL with the absolute file URL for
your checkout (on Windows, for example, `file:///C:/src/wayang/wayang-profiler/src/main/resources/duckdb-ga.properties`). No platform-specific launch script or manually assembled Java
classpath is required.

The settings in `src/main/resources/duckdb-ga.properties` control the run limits
and output location. By default, learned coefficients are written to
`wayang-platforms/wayang-duckdb/target/cost-profiling/duckdb/learned-duckdb-relaxed.properties`.
The optimizer is stochastic, so successive runs can produce different coefficients.
