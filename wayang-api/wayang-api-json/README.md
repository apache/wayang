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
  
# Wayang JSON REST API

## Getting Started

### 1. Package the Project

```bash
./mvnw clean package -pl :wayang-assembly -Pdistribution
```

### 2. Starting the REST API

```bash
cd wayang-assembly/target/
tar -xvf wayang-assembly-1.1.2-SNAPSHOT-dist.tar.gz
cd wayang-1.1.2-SNAPSHOT
./bin/wayang-submit org.apache.wayang.api.json.Main
```

## Collection responses

`POST /wayang-api-json/submit-plan/json` accepts an optional top-level
`"collect": true` field. Such plans must have exactly one terminal operator and
no output sink. Execution uses the existing `DataQuanta.collect()` sink and
returns `application/json` with `{"results": [...]}`, including an empty array
for empty results. Failures use the existing HTTP 400 error response.

Result values may be strings, booleans, signed 64-bit integers, finite floats,
and nested lists, arrays, or pairs. Pairs become JSON arrays. Other objects and
null values are rejected; results never use Java object serialization or
Scala expression evaluation.

PyWayang marks a terminal `mapPartitions` with `data.jsonOutput: true` when its
UDF emits UTF-8 JSON records through the existing length-prefixed worker stream.
The REST handler decodes these records as JSON data before constructing the
response. This marker is not needed for native JVM results such as collection
sources. It does not change intermediate operators or legacy worker encoding.
If that UDF fails, its final record is `{"error": "ExceptionType: message"}`;
the handler fails the entire request rather than returning partial results.
Mappings are not supported result values, so this record cannot be mistaken
for a user value.

Omitting `collect` (or setting it to false) preserves the existing text response:
`Success` for output sinks, and the Scala collection representation otherwise.
The Drawflow endpoint is unchanged. Collection responses are held in memory and
do not guarantee ordering beyond that provided by the selected execution plan.
