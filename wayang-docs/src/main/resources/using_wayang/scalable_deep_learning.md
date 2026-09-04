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
title: "Scalable Deep Learning"
previous:
    url: /using_wayang/cost_model_calibration/
    title: Cost Model Calibration
next:
    url: /how_contribute/
    title: How To Contribute
menu:
    using:
        weight: 7
---

# Scalable Deep Learning in Apache Wayang

Apache Wayang provides first-class support for scalable deep learning, bridging big data processing platforms with deep learning frameworks.

Through the `wayang-tensorflow` platform module and core deep learning abstractions, Wayang enables distributed data preprocessing (e.g., via Spark or Java Streams) combined seamlessly with GPU/CPU accelerated neural network training and batch inference.

---

## Core Abstractions

### 1. `DLModel`
`org.apache.wayang.basic.model.DLModel` represents a deep neural network model. It encapsulates the computational graph, layer definitions (e.g., Dense/Linear, Conv2D, Conv3D, BatchNorm, ConvLSTM), and trainable weights.

### 2. `DLTrainingOperator`
`DLTrainingOperator` trains a deep neural network model on an input dataset. It takes training features and labels as input streams or tensors, executes training epochs with specified loss functions and optimizers, and produces an updated `DLModel`.

### 3. `PredictOperator`
`PredictOperator` performs high-throughput batch inference. It applies an existing `DLModel` to incoming input data tensors and produces predicted outputs or probability distributions.

---

## The TensorFlow Platform (`wayang-tensorflow`)

Wayang includes a dedicated platform adapter for TensorFlow (`wayang-tensorflow`), utilizing Java bindings and native acceleration to execute deep learning operators on CPUs and GPUs.

### Maven Dependency
To use TensorFlow capabilities in your Wayang application, add the following dependency:

```xml
<dependency>
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-tensorflow</artifactId>
  <version>${wayang.version}</version>
</dependency>
```

> Replace `${wayang.version}` with a released version from Maven Central (e.g., `1.1.1`) or the latest development snapshot version (e.g., `1.1.2-SNAPSHOT` with Apache Snapshots repository configured).

---

## End-to-End Deep Learning Workflow Example

The following example demonstrates setting up an integrated pipeline that loads and preprocesses data before executing inference using `TensorflowPlugin`:

```java
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.tensorflow.Tensorflow;
import org.apache.wayang.tensorflow.model.TensorflowModel;

public class DeepLearningPipeline {

    public static void main(String[] args) {
        // 1. Initialize WayangContext with Java, Spark, and TensorFlow plugins
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Tensorflow.plugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext);

        // 2. Load and preprocess input features using distributed Spark / Java
        // 3. Connect dataflow to TensorFlow execution operators
        // Wayang automatically handles tensor serialization and platform conversions
    }
}
```

### Supported Neural Network Layers
`wayang-tensorflow` provides built-in operators for constructing modular architectures:
- **Fully Connected**: `TensorflowLinear`
- **Convolutional**: `TensorflowConv2D`, `TensorflowConv3D`
- **Recurrent & Spatio-Temporal**: `TensorflowConvLSTM2D`
- **Normalization**: `TensorflowBatchNorm2D`, `TensorflowBatchNorm3D`

---

## Benefits of Wayang's Deep Learning Integration

- **Zero Hand-Coded Glue**: Wayang automatically generates data conversion channels (`TensorChannel`) between data preparation platforms (Java/Spark) and deep learning runtimes.
- **Hardware Agnostic**: Run inference or training locally during prototyping and scale out across cluster GPUs in production without rewriting data transformation logic.
