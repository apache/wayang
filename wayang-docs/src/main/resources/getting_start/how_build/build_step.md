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
title: "Step by Step Building Wayang"
previous:
    url: /getting_start/how_build/
    title: How to Build Wayang
next:
    url: /getting_start/how_install/
    title: How to Install Wayang
menu:
    build:
        weight: 1
---

# Step by Step Building Wayang

Follow these step-by-step instructions to compile, test, and package Apache Wayang on your machine.

---

### Step 1: Clone the Git Repository
```shell
$ git clone https://github.com/apache/wayang.git
$ cd wayang
```

---

### Step 2: Verify System Prerequisites
Ensure that your `JAVA_HOME` points to JDK 17 and that Java is in your system PATH:

```shell
$ java -version
# Expected: openjdk version "17.0.x"
```

---

### Step 3: Compile and Install Core Libraries
Build the core framework and platform adapters using the included Maven wrapper:

```shell
$ ./mvnw clean install -DskipTests
```

This compiles all modules into your local Maven cache (`~/.m2/repository`).

---

### Step 4: Building Specific Modules
You can build individual sub-modules to accelerate development workflows:

```shell
# Build only core and basic modules
$ ./mvnw clean install -pl wayang-commons/wayang-core,wayang-commons/wayang-basic -DskipTests

# Build the Spark platform adapter
$ ./mvnw clean install -pl wayang-platforms/wayang-spark -DskipTests
```

---

### Step 5: Assembling Binary Distributions
To assemble the full redistributable binary tarball containing executable scripts and dependencies:

```shell
$ ./mvnw clean package -Pdistro -DskipTests
```

The resulting archives are generated in `wayang-assembly/target/`.
