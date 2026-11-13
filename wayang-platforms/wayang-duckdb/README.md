# Wayang Platform DuckDB

Wayang platform adapter for [DuckDB](https://duckdb.org/) through JDBC.

DuckDB is embedded, so the platform does not require a coordinator, worker, or
long-running database service. Use `jdbc:duckdb:` for an in-memory database, or
`jdbc:duckdb:/path/to/database.duckdb` for a persistent database file.

## Usage

Register the DuckDB plugin in a Wayang context:

```java
Configuration config = new Configuration();
config.setProperty("wayang.duckdb.jdbc.url", "jdbc:duckdb:/tmp/wayang.duckdb");

WayangContext wayang = new WayangContext(config)
        .withPlugin(DuckDB.plugin());
```

The default configuration lives in
`src/main/resources/wayang-duckdb-defaults.properties`. Important properties:

```properties
wayang.duckdb.jdbc.url = jdbc:duckdb:
wayang.duckdb.jdbc.user =
wayang.duckdb.jdbc.password =
```

## Supported Operators

The DuckDB platform follows the same JDBC pushdown model as Trino and Presto:

| Operator | SQL shape |
|----------|-----------|
| `TableSource` | `SELECT * FROM table` |
| `Filter` | `WHERE ...` |
| `Projection` | `SELECT col1, col2, ...` |
| `Join` | `JOIN ... ON ...` |
| `GlobalReduce` | aggregate projection such as `SUM(amount)` |
| `ReduceBy` | aggregate projection plus `GROUP BY` |
| `Sort` | `ORDER BY ...` |
| `TableSink` | `CREATE TABLE ... AS SELECT ...` or `INSERT INTO ... SELECT ...` |
| `ParquetSource` | DuckDB relation, configured mapping, or auto-created `read_parquet(...)` view |

## Parquet And GCS

DuckDB can read local Parquet files directly through `read_parquet(...)`. The
Wayang adapter supports two Trino/Presto-style modes:

```properties
# Keep the logical Parquet URI and map it to an existing DuckDB relation.
wayang.duckdb.parquetsource.mappings = file:///data/orders.parquet=wayang_parquet.orders

# Or let DuckDB create a view over the Parquet location before execution.
wayang.duckdb.parquetsource.auto-create = true
wayang.duckdb.parquetsource.auto-create.template = CREATE OR REPLACE VIEW ${relation} AS SELECT * FROM read_parquet('${uri}')
```

For GCS Parquet files, load DuckDB's `httpfs` extension before the view is
created:

```properties
wayang.duckdb.parquetsource.prepare-sql = INSTALL httpfs; LOAD httpfs
```

## Tests

The embedded operator suite mirrors `TrinoOperatorsIT` / `PrestoOperatorsIT`,
but runs against a temporary DuckDB database file. Separate Parquet and cost
profiling suites cover DuckDB-specific file access and calibration workflows.

Run the DuckDB operator suite while compiling the required reactor modules:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-duckdb -am \
  -Dtest=DuckDBOperatorsIT \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false \
  -Drat.skip=true -Dlicense.skip=true test
```

On PowerShell:

```powershell
.\mvnw.cmd --% -Pskip-prerequisite-check -pl wayang-platforms/wayang-duckdb -am -Dtest=DuckDBOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false -Drat.skip=true -Dlicense.skip=true test
```

Expected result:

```text
Tests run: 15, Failures: 0, Errors: 0, Skipped: 0
```

Run the Parquet suite, including a public GCS smoke when DuckDB `httpfs` can
reach the configured object:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-duckdb -am \
  -Dtest=DuckDBParquetSourceIT \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false \
  -Drat.skip=true -Dlicense.skip=true test
```

Override the public GCS file with:

```bash
-Dduckdb.gcs.parquet.uri=gs://bucket/path/file.parquet
```

## Test Coverage

| Test | What it checks |
|------|----------------|
| `loadsDuckDbDriverAndRunsQuery` | DuckDB JDBC driver sanity check |
| `tableSource` | Full table scan through `DuckDBTableSource` |
| `filter` | Wayang `FilterOperator` and SQL `WHERE` pushdown |
| `projection` | Column projection through SQL `SELECT` |
| `join` | DuckDB join plus a test-only flatten projection |
| `globalReduce` | Global aggregation such as `SUM` |
| `reduceBy` | Grouped aggregation and SQL `GROUP BY` |
| `sort` | Wayang sort and SQL `ORDER BY` |
| `tableSink` | Filtered result written with `CREATE TABLE AS` |
| `javaPlanBuilderReadTableFilterProjection` | `readTable -> filter -> projection -> writeTable` |
| `javaPlanBuilderReadTableFilterGlobalReduce` | `readTable -> filter -> globalReduce -> writeTable` |
| `javaPlanBuilderReadTableReduceBySort` | `readTable -> reduceByKey -> sort -> writeTable` |
| `javaPlanBuilderReadTableFilterProjectionTableSink` | `readTable -> filter -> projection -> writeTable` |
| `javaPlanBuilderReadTableJoin` | `readTable + readTable -> join -> writeTable` |
| `generatedSqlContainsPushdownShapes` | Captured SQL contains `WHERE`, `JOIN`, `GROUP BY`, and `ORDER BY` |

## Cost Profiling

`DuckDBCostPilotIT` follows the Trino Week8 cost-pilot shape and writes Wayang
execution/cardinality logs plus a manifest under `target/cost-profiling/duckdb`.
The default/reference workload is S01-S13 over 10k, 50k, 100k, and 250k rows
with six repetitions, producing 312 Wayang executions:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-duckdb -am \
  -Dtest=DuckDBCostPilotIT#runPilot \
  -Dduckdb.profile.rowCounts=10000,50000,100000,250000 \
  -Dduckdb.profile.plans=S01,S02,S03,S04,S05,S06,S07,S08,S09,S10,S11,S12,S13 \
  -Dduckdb.profile.repetitions=6 \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false \
  -Drat.skip=true -Dlicense.skip=true test
```

S14-S16 are implemented as optional expanded/join-heavy plans and can be passed
explicitly, but the checked-in reference parameters come from the Week8 S01-S13
run.

For a quick local smoke:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-duckdb -am \
  -Dtest=DuckDBCostPilotIT#runPilot \
  -Dduckdb.profile.rowCounts=100,1000 \
  -Dduckdb.profile.plans=S01,S02 \
  -Dduckdb.profile.repetitions=2 \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false \
  -Drat.skip=true -Dlicense.skip=true test
```

The GA configuration is maintained in
[`wayang-profiler`](../../wayang-profiler/duckdb.md), which documents how to run
calibration with the generated execution log on Linux, macOS, and Windows.

## Example

The configurable filter and projection example lives in
[`wayang-applications`](../../wayang-applications/duckdb.md).
It connects directly through JDBC; no Docker setup is required.
