<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Cost Profiling Guide

This document explains why Apache Wayang needs platform-specific cost
profiling, how profiling data is collected, how the genetic optimizer learns
cost parameters, and how users can repeat the profiling workflow on their own
hardware.

The examples below use Trino, but the same workflow also applies to other
JDBC-based platforms such as Presto and BigQuery.

Version 2.0 uses S01 through S16 as the profiling workload, including the
join-heavy pipelines S14 through S16. It keeps the guide focused on data
collection and parameter learning, leaving follow-up quality checks out of
scope for now.

## 1. Why Profiling Is Needed

Wayang can map the same logical plan to different execution platforms, such as
Java, Spark, Trino, Presto, or BigQuery. For example, a user query may contain:

```text
TableSource -> Filter -> Projection -> TableSink
```

The optimizer needs a cost model to decide whether these operators should stay
on a SQL platform or be moved to another platform. In this context, "cost" does
not mean cloud billing cost. It is the numerical value that Wayang uses to
compare alternative execution plans.

With the default Trino configuration:

```properties
wayang.trino.costs.fix = 0.0
wayang.trino.costs.per-ms = 1.0
```

the optimizer cost can be interpreted approximately as:

```text
cost = estimated execution time in milliseconds
```

However, the real execution time depends on the user's machine, cluster size,
network, database configuration, and workload. Therefore, users should profile
their own environment when they need accurate cost parameters.

## 2. Load Profile Formulas

Each execution operator has a load profile. For example, a table source may use
a formula like:

```properties
wayang.trino.tablesource.load = {
  "type":"mathex",
  "in":0,
  "out":1,
  "cpu":"((10)*(out0))+(800000)",
  "ram":"0",
  "disk":"0",
  "net":"0",
  "p":0.9
}
```

This can be read as:

```text
CPU load = 10 * number_of_rows + 800000
```

where:

- `out0` is the output cardinality.
- `10` is the per-row cost.
- `800000` is the fixed overhead, such as query planning, scheduling, and remote
  execution startup.
- `p` is the confidence of the estimate.

In template form, these constants are the unknown parameters:

```text
CPU load = alpha * number_of_rows + beta
```

The profiling goal is to learn reasonable values for `alpha` and `beta` from
real execution records.

Wayang can also define templates with unknown parameters:

```properties
wayang.trino.tablesource.load.template = {
  "type":"mathex",
  "in":0,
  "out":1,
  "cpu":"?*out0 + ?",
  "ram":"0",
  "disk":"0",
  "net":"0",
  "p":0.9
}
```

The genetic optimizer reads the templates and replaces the `?` placeholders with
learned values.

## 3. Profiling Workflow

The expected profiling workflow is:

```text
Run Wayang jobs with different operators and input cardinalities
                    |
                    v
Record platform, operator lineage, cardinalities, and runtime
                    |
                    v
              executions.json
                    |
                    v
      GeneticOptimizerApp reads the execution records
                    |
                    v
Learn the unknown parameters in *.load.template
                    |
                    v
Write learned *.load formulas
```

Wayang stores measured executions as `PartialExecution` records. Each record
contains:

- the measured execution time;
- the platform that executed the stage;
- one or more `ExecutionLineageNode` objects;
- the load profile estimator for each profiled operator;
- input and output cardinalities.

The default execution log location is usually:

```text
~/.wayang/executions.json
```

For controlled profiling experiments, it is better to write the log to a
dedicated experiment folder, for example:

```text
C:\Users\<user>\Desktop\Wayang Profiling\trino\week8\executions.json
```

## 4. Experiment Design

Profiling should include both single-operator pipelines and combined pipelines.
Single-operator pipelines help isolate each operator. Combined pipelines help
the optimizer learn parameters from realistic SQL stages, where multiple
operators are executed together.

Choose input cardinalities according to the machine or cluster being profiled.
The values below are only an example that can run on a laptop-sized local
setup:

```text
10k, 50k, 100k, 250k
```

For a smaller machine, use fewer or smaller cardinalities. For a larger local
or remote platform, add larger cardinalities so the learned model reflects the
scale that users expect to run.

Recommended repetitions:

```text
1 warm-up run + 5 measured runs
```

Profiling pipelines:

| Plan | Pipeline |
|------|----------|
| S01 | TableSource -> TableSink |
| S02 | TableSource -> Filter(50%) -> TableSink |
| S03 | TableSource -> Projection(order_id, amount) -> TableSink |
| S04 | TableSource -> Filter(50%) -> Projection(order_id, amount) -> TableSink |
| S05 | TableSource -> GlobalReduce(sum amount) -> TableSink |
| S06 | TableSource -> ReduceBy(bucket) -> TableSink |
| S07 | TableSource -> Sort(amount) -> TableSink |
| S08 | Orders -> Join(Customers 1k) -> Projection -> TableSink |
| S09 | TableSource -> Filter(50%) -> GlobalReduce -> TableSink |
| S10 | TableSource -> Filter(50%) -> ReduceBy -> TableSink |
| S11 | TableSource -> Filter(50%) -> Sort(amount) -> TableSink |
| S12 | TableSource -> Projection(order_id, amount) -> Sort(amount) -> TableSink |
| S13 | TableSource -> Filter(50%) -> Projection(order_id, amount) -> Sort(amount) -> TableSink |
| S14 | Orders -> Filter(50%) -> Join(Customers 1k) -> Projection -> TableSink |
| S15 | Orders -> Join(Customers 1k) -> Projection(order_id, tier, amount) -> Sort(amount) -> TableSink |
| S16 | Orders -> Join(Customers 1k) -> Projection(tier, amount) -> ReduceBy(tier) -> TableSink |

S01 through S16 should be treated as one profiling workload, including the
join-heavy plans S14 through S16. For example, using 16 plans, 4 cardinalities,
and 6 repetitions produces:

```text
16 * 4 * 6 = 384 Wayang executions
```

If users choose a different number of cardinalities or repetitions, the total
number of executions changes accordingly:

```text
number_of_plans * number_of_cardinalities * repetitions
```

The reference parameters shipped in the platform defaults were learned from our
local Week 8 profiling runs over S01 through S13, with row counts
10k/50k/100k/250k and 1 warm-up plus 5 measured repetitions. S14 through S16
were added to this guide to document the join-heavy pipelines that users should
include when they rerun profiling in their own environment. The shipped
parameters are intended as reasonable starting values for users who just want
to try Wayang; they are not universal parameters for every deployment.

## 5. Benchmarking Rules

To reduce measurement noise:

1. Create test data before the measured run. Do not include fixture setup time
   in operator duration.
2. Run at least one warm-up execution for each plan/cardinality pair.
3. Repeat each measured scenario multiple times.
4. Store every individual measurement instead of storing only averages.
5. Record exact input and output cardinalities.
6. Keep platform settings stable, including worker count, JVM settings, memory
   limits, and connector configuration.
7. Record abnormal runs, such as failures caused by GC, cold cache, network
   issues, or competing workloads.

For distributed systems such as Trino, it is also important to define what the
model should predict:

- If Wayang should predict user-visible runtime, fit wall-clock elapsed time.
- If the platform reports CPU time and the model uses CPU load, make sure the
  conversion to Wayang cost is consistent with the resource model.
- Parameters learned on one deployment should be treated as environment-specific
  reference values, not universal defaults for every deployment.

## 6. Running a Profiling Experiment

The exact command depends on the platform module, test class, and property
prefix.

| Platform | Platform guide | Maven module | Test class | Property prefix | Default output directory |
|----------|-------------|--------------|------------|-----------------|--------------------------|
| Trino | `wayang-platforms/wayang-trino/README.md` | `wayang-platforms/wayang-trino` | `TrinoCostPilotIT` | `trino.profile.*` | `target/cost-profiling/trino` |
| Presto | `wayang-platforms/wayang-presto/README.md` | `wayang-platforms/wayang-presto` | `PrestoCostPilotIT` | `presto.profile.*` | `target/cost-profiling/presto` |
| BigQuery | `wayang-platforms/wayang-bigquery/README.md` | `wayang-platforms/wayang-bigquery` | `BigQueryCostPilotIT` | `bigquery.profile.*` | `target/cost-profiling/bigquery` |

The commands below use PowerShell. On macOS/Linux, use `./mvnw` instead of
`.\mvnw.cmd` and replace PowerShell backticks with Bash line-continuation
backslashes.

Trino:

```powershell
.\mvnw.cmd -Pskip-prerequisite-check -pl wayang-platforms/wayang-trino -am `
  "-Dtest=TrinoCostPilotIT" `
  "-Dsurefire.failIfNoSpecifiedTests=false" `
  "-DfailIfNoTests=false" `
  "-Dtrino.profile.outputDir=target/cost-profiling/trino" `
  "-Dtrino.profile.rowCounts=10000,50000,100000,250000" `
  "-Dtrino.profile.plans=S01,S02,S03,S04,S05,S06,S07,S08,S09,S10,S11,S12,S13,S14,S15,S16" `
  "-Dtrino.profile.repetitions=6" `
  "-Dtrino.profile.reset=true" `
  "-Drat.skip=true" `
  "-Dlicense.skip=true" `
  "-Dmaven.javadoc.skip=true" `
  test
```

Presto:

```powershell
.\mvnw.cmd -Pskip-prerequisite-check -pl wayang-platforms/wayang-presto -am `
  "-Dtest=PrestoCostPilotIT" `
  "-Dsurefire.failIfNoSpecifiedTests=false" `
  "-DfailIfNoTests=false" `
  "-Dpresto.profile.outputDir=target/cost-profiling/presto" `
  "-Dpresto.profile.rowCounts=10000,50000,100000,250000" `
  "-Dpresto.profile.plans=S01,S02,S03,S04,S05,S06,S07,S08,S09,S10,S11,S12,S13,S14,S15,S16" `
  "-Dpresto.profile.repetitions=6" `
  "-Dpresto.profile.reset=true" `
  "-Drat.skip=true" `
  "-Dlicense.skip=true" `
  "-Dmaven.javadoc.skip=true" `
  test
```

BigQuery:

```powershell
.\mvnw.cmd -Pskip-prerequisite-check -pl wayang-platforms/wayang-bigquery -am `
  "-Dtest=BigQueryCostPilotIT" `
  "-Dsurefire.failIfNoSpecifiedTests=false" `
  "-DfailIfNoTests=false" `
  "-Dbigquery.project=YOUR_PROJECT_ID" `
  "-Dbigquery.saEmail=wayang-bq@YOUR_PROJECT_ID.iam.gserviceaccount.com" `
  "-Dbigquery.keyPath=C:\path\to\wayang-bq-key.json" `
  "-Dbigquery.location=US" `
  "-Dbigquery.profile.outputDir=target/cost-profiling/bigquery" `
  "-Dbigquery.profile.rowCounts=10000,50000,100000,250000" `
  "-Dbigquery.profile.plans=S01,S02,S03,S04,S05,S06,S07,S08,S09,S10,S11,S12,S13,S14,S15,S16" `
  "-Dbigquery.profile.repetitions=6" `
  "-Dbigquery.profile.reset=true" `
  "-Drat.skip=true" `
  "-Dlicense.skip=true" `
  "-Dmaven.javadoc.skip=true" `
  test
```

Expected output files:

| File | Purpose |
|------|---------|
| `executions.json` | Wayang execution records consumed by the GA profiler |
| `manifest.csv` | Human-readable mapping from run ID to plan, cardinality, repetition, and status |

## 7. Running the Genetic Optimizer

The entry point for learning cost parameters is:

```text
org.apache.wayang.profiler.log.GeneticOptimizerApp
```

A typical profiling configuration contains:

- platform default properties;
- `wayang.<platform>.*.load.template` formulas;
- GA settings;
- the path to `executions.json`;
- the output path for learned parameters.

Example GA settings:

```properties
wayang.profiler.ga.timelimit.ms = 120000
wayang.profiler.ga.maxgenerations = 800
wayang.profiler.ga.maxstablegenerations = 150
wayang.profiler.ga.superoptimizations = 1
wayang.profiler.ga.intermediateupdate = 200
wayang.profiler.ga.min-exec-time = 1
wayang.profiler.ga.max-cardinality-spread = 100
wayang.profiler.ga.min-cardinality-confidence = 0
wayang.profiler.ga.binning = 1.0
wayang.profiler.ga.output-file = <output-file>
```

The profiler writes learned formulas such as:

```properties
wayang.trino.tablesource.load = ...
wayang.trino.filter.load = ...
wayang.trino.join.load = ...
```

## 8. Cardinality Estimation Note

For JDBC-based table sources, `JdbcTableSource#getCardinalityEstimator` may open
a JDBC connection and run:

```sql
SELECT count(*) FROM <table>
```

This is used during Wayang's optimization phase to estimate source
cardinalities.

Important details:

- The estimator is not called for every registered platform.
- It is called only for operators that appear in the current Wayang plan or plan
  implementation being estimated.
- If the current plan contains a Trino, Presto, or BigQuery table source, the
  corresponding JDBC cardinality estimator may run.
- If the count query fails, the current implementation falls back to a
  conservative estimate.

For cloud platforms, this extra count query can add overhead or fail because of
network or authentication issues. For profiling, it can be useful to support
cached or user-provided source cardinalities in the future.

## 9. Completion Criteria

A profiling run is complete when:

- the platform execution stage records a `PartialExecution`;
- `executions.json` contains the expected platform;
- execution records contain estimator keys for relevant operators such as
      `tablesource`, `filter`, `projection`, `join`, `reduceby`, `sort`, and
      `tablesink`;
- input and output cardinalities are available;
- `GeneticOptimizerApp` can read the execution log;
- the profiler outputs learned platform load formulas;
- the learned formulas and experiment settings are documented together so they
  can be interpreted as environment-specific profiling results.

## 10. Recommended Implementation Order

When adding profiling support for a new platform, a conservative order is:

1. Create a minimal proof of concept for one stage, for example
   `TableSource -> Filter -> TableSink`.
2. Confirm that `executions.json` contains the correct platform, estimator keys,
   cardinalities, and measured duration.
3. Make sure the profiler can initialize the platform and deserialize its
   execution records.
4. Run a small benchmark and generate candidate parameters.
5. Extend the workload to all important operators and combined pipelines.
6. Decide whether the learned parameters should become reference defaults or
   remain documented as environment-specific profiling results.
