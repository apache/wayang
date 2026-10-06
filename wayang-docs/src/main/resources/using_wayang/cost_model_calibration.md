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
title: "Cost Model Calibration"
previous:
    url: /using_wayang/configuring_wayang/
    title: Configuring Wayang
next:
    url: /using_wayang/scalable_deep_learning/
    title: Scalable Deep Learning
menu:
    using:
        weight: 6
---

# Cost Model Calibration

Apache Wayang incorporates an advanced cost-based optimizer that evaluates possible execution plans and selects the most efficient combination of execution platforms for your workload.

To accurately estimate execution time and resource utilization across heterogeneous platforms, Wayang utilizes **Load Profile Estimators** for both built-in operators and User-Defined Functions (UDFs).

---

## Load Profile Estimator Templates

Wayang allows specifying load profile estimator configurations through mathematical templates. For instance, the Java map operator obtains its load profile configuration via `wayang.java.map.load`.

You can supply a template using the `<key>.template` configuration property:

```properties
wayang.java.map.load.template = {\
  "in":1, "out":1,\
  "cpu":"?*in0"\
}
```

### Template Elements
- **Input and Output Quantities**: `"in": 1, "out": 1` declares the number of inputs and outputs expected by the operator.
- **Cardinality Variables**: `in0`, `in1`, ... and `out0`, `out1`, ... represent the input and output cardinalities.
- **Operator Properties**: Access operator properties like `numIterations` for iterative operators (e.g., PageRank).
- **Operators & Arithmetic**: Use standard operations `+`, `-`, `*`, `/`, `%`, `^`, and parentheses.
- **Built-in Functions**:
  - `min(x0, x1, ...)`
  - `max(x0, x1, ...)`
  - `abs(x)`
  - `log(x, base)`, `ln(x)`, `ld(x)`
- **Mathematical Constants**: `e` and `pi`.

---

## Calibrating with the Genetic Optimizer

When unknown parameters (`?`) are left in your estimator templates, Wayang can learn and calibrate the coefficients from historical execution logs using genetic optimization.

### 1. Enable Execution Logging
Ensure execution logging is enabled in your `wayang.properties`:

```properties
wayang.core.log.enabled = true
wayang.core.log.executions = file:///path/to/executions.json
wayang.core.log.cardinalities = file:///path/to/cardinalities.json
```

### 2. Run the Calibration Utility
Once execution data has been collected from representative application runs, execute the calibration tool:

```shell
$ java -cp ... org.apache.wayang.profiler.ga.GeneticOptimizerApp \
    file:///path/to/wayang.properties \
    file:///path/to/executions.json
```

The genetic optimizer will evaluate the collected run times against candidate coefficients and output fitted values replacing the question marks (`?`).

### 3. Apply Calibrated Values
Copy the fitted coefficients directly into your production configuration properties to enable highly accurate cost-based plan selection tailored to your specific hardware cluster.
