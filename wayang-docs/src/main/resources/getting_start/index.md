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
title: "Getting Started"
previous:
    url: /
    title: Read Me
next:
    url: /getting_start/how_build/
    title: How to Build Wayang
menu:
    header:
        weight: 2
---

# Getting Started with Apache Wayang

Welcome to Apache Wayang! This section guides you through the initial steps to get up and running, build the system, understand core concepts, and assemble your first cross-platform data processing application.

---

## Getting Started Sections

1. **[How to Build Wayang]({% link getting_start/how_build/index.md %})**  
   Prerequisites, system requirements (Java 17, Scala 2.12, Maven), and building Wayang from source using standard profiles.

2. **[How to Install Wayang]({% link getting_start/how_install/index.md %})**  
   Adding Wayang dependencies to your Maven or Gradle builds and configuring artifact repositories.

3. **[How to Run Wayang]({% link getting_start/how_run/index.md %})**  
   Running Wayang applications and executing binaries via the CLI submission tools.

4. **[Wayang Abstractions & Plans]({% link getting_start/writting_wayang_plan/index.md %})**  
   Understanding Wayang's fundamental operator abstractions (Source, Unary, Binary, Loop, Sink) and constructing pipelines using `JavaPlanBuilder`.

---

## API Documentation

For the full reference of classes, packages, and interfaces, explore the official [Wayang API JavaDocs](https://wayang.apache.org/docs/api/javadocs/).
