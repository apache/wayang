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
title: "How to Build Wayang"
previous:
    url: /getting_start/
    title: Getting Started
next:
    url: /getting_start/how_build/build_step/
    title: Step by Step Building Wayang
menu:
    getting_start:
        weight: 0
---

# How to Build Apache Wayang

This guide details the system prerequisites, environment setup, and Maven commands required to build Apache Wayang from source.

---

## Requirements

Before building Apache Wayang from source, ensure your environment meets the following specifications:

- **Java Development Kit (JDK)**: JDK 17.
- **Scala**: Version 2.12.x.
- **Apache Maven**: Maven 3.8.0 or newer (or use the included Maven wrapper `./mvnw` / `mvnw.cmd`).
- **Platform Prerequisites**:
  - **Linux / macOS**: Standard development toolchains (`tar`, `gzip`).
  - **Windows**: Requires Hadoop winutils binaries located in `%HADOOP_HOME%\bin\winutils.exe` if running Hadoop/Spark integration locally.

---

## Building from Source

### Quick Build (Skipping Tests)
To build all Wayang modules and compile JARs without running test suites:

```shell
$ git clone https://github.com/apache/wayang.git
$ cd wayang
$ ./mvnw clean install -DskipTests
```

### Full Build with Tests
To run unit and platform integration tests:

```shell
$ ./mvnw clean install
```

---

## Build Profiles

Wayang provides specialized Maven build profiles for assembling distributions and targeting execution environments:

| Profile | Command | Purpose |
|---|---|---|
| `distro` | `./mvnw clean install -Pdistro` | Assembles the complete binary release archive in `wayang-assembly`. |
| `standalone` | `./mvnw clean install -Pstandalone` | Packages bundled dependencies so standalone applications do not need external cluster libraries. |
| `web-documentation` | `./mvnw site -pl wayang-docs -Pweb-documentation` | Builds the Jekyll documentation site. |

For step-by-step guidance, see [Step by Step Building Wayang]({% link getting_start/how_build/build_step.md %}).
