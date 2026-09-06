<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0.
  You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0.
-->

# Wayang Platform Presto

This module connects Wayang to a user-managed PrestoDB deployment through JDBC.
It does not require Docker or a repository-provided Presto environment.

Configure the connection in a Wayang properties file:

```properties
wayang.presto.jdbc.url = jdbc:presto://presto.example.com:8080/catalog/schema
wayang.presto.jdbc.user = wayang
```

Add `wayang.presto.jdbc.password` only when the deployment requires one.

For a complete table definition, sample data, and runnable Wayang plan, see
[`wayang-applications/presto.md`](../../wayang-applications/presto.md).

## Integration tests

`PrestoOperatorsIT` is intended for connector development. It requires a
writable `memory` catalog and permission to read `system.runtime.queries`. The
test creates the `memory.wayang_it` schema and its fixture tables, exercises the
supported JDBC operators, and removes the fixtures afterward.

Set the endpoint when it differs from the defaults shown below, then run the
test from the repository root:

```bash
PRESTO_HOST=localhost PRESTO_PORT=8080 PRESTO_USER=test \
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-presto -am \
  -Dtest=PrestoOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
  -DfailIfNoTests=false test
```

These environment variables configure the integration test only. Applications
use the `wayang.presto.jdbc.*` properties described above.

Cost calibration uses `PrestoCostPilotIT`; see
[`guides/cost-profiling.md`](../../guides/cost-profiling.md).
