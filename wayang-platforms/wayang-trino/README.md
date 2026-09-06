<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0.
  You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0.
-->

# Wayang Platform Trino

This module connects Wayang to a user-managed Trino deployment through JDBC.
Configure the endpoint and credentials in a Wayang properties file:

```properties
wayang.trino.jdbc.url = jdbc:trino://trino.example.com:8080/catalog/schema
wayang.trino.jdbc.user = wayang
wayang.trino.jdbc.password =
```

The runnable filter/projection example and its table requirements are documented
in [`wayang-applications/trino.md`](../../wayang-applications/trino.md).

## Integration tests

`TrinoOperatorsIT` is intended for connector development. It requires a
writable catalog named `iceberg`, support for Parquet tables, permission to
create the `iceberg.wayang_it` schema, and permission to read
`system.runtime.queries`. It creates and removes its own fixture tables.

Set the endpoint when it differs from the defaults shown below, then run the
test from the repository root:

```bash
TRINO_HOST=localhost TRINO_PORT=8080 TRINO_USER=admin \
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-trino -am \
  -Dtest=TrinoOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
  -DfailIfNoTests=false test
```

These environment variables configure the integration test only. Applications
use the `wayang.trino.jdbc.*` properties described above.

Cost calibration uses `TrinoCostPilotIT`; see
[`guides/cost-profiling.md`](../../guides/cost-profiling.md).
