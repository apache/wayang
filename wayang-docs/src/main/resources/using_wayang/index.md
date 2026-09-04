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
title: "Using Wayang"
previous:
    url: /getting_start/writting_wayang_plan/
    title: Wayang Abstractions & Plans
next:
    url: /using_wayang/configuring_wayang/
    title: Configuring Wayang
menu:
    header:
        weight: 3
---

# Using Apache Wayang

Apache Wayang offers a variety of language APIs, execution adapters, configuration facilities, and optimization utilities to suit different data processing workflows.

---

## User Guides & APIs

### 1. Developer APIs
- **[API Java & Scala]({% link using_wayang/api_java_scala/index.md %})**: Write type-safe cross-platform data processing pipelines using fluent `PlanBuilder` and lambda transformations.
- **[API Python (PyWayang)]({% link using_wayang/api_python/index.md %})**: Author pipelines in Python with native Python functions and cross-language execution.
- **[API SQL]({% link using_wayang/api_sql/index.md %})**: Submit relational SQL queries optimized by Wayang across heterogeneous backends.
- **[API REST]({% link using_wayang/api_rest/index.md %})**: Submit and monitor Wayang plans via HTTP endpoints.
- **[API JDBC]({% link using_wayang/api_jdbc/index.md %})**: Connect database clients and BI tools to Wayang.

---

### 2. Configuration & Advanced Capabilities
- **[Configuring Wayang]({% link using_wayang/configuring_wayang.md %})**: Complete reference of system properties, platform parameters, and tuning flags.
- **[Cost Model Calibration]({% link using_wayang/cost_model_calibration.md %})**: Calibrating load profile estimator templates with execution logs using the genetic algorithm optimizer.
- **[Scalable Deep Learning]({% link using_wayang/scalable_deep_learning.md %})**: Deep learning model training and inference pipelines with `DLModel` and the TensorFlow platform adapter.

---

## API JavaDocs Reference

For exhaustive class, method, and package-level documentation, visit the [Wayang API JavaDocs](https://wayang.apache.org/docs/api/javadocs/).
