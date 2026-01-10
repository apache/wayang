<!--

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

-->
# Using Machine-Learning for query optimization in Apache Wayang 
Apache Wayang can be customized with concrete
implementations of the `EstimatableCost` interface in order to optimize
for a desired metric.  The implementation can be enabled by providing it
to a `Configuration`.

```java
public class CustomEstimatableCost implements EstimatableCost {
    /* Provide concrete implementations to match desired cost function(s)
     * by implementing the interface in this class.
     */
}
public class WordCount {
    public static void main(String[] args) {
        /* Create a Wayang context and specify the platforms Wayang will consider */
        Configuration config = new Configuration();
        /* Provision of a EstimatableCost that implements the interface.*/
        config.setCostModel(new CustomEstimatableCost());
        WayangContext wayangContext = new WayangContext(config)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin());
        /*... omitted */
    }
}
```

In combination with an encoding scheme and a third party package to load
ML models, the following example shows how to predict runtimes of query
execution plans runtimes in Apache Wayang:

```java
import org.apache.wayang.core.optimizer.costs.EstimatableCost;
import org.apache.wayang.core.optimizer.costs.EstimatableCostFactory;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.enumeration.LoopImplementation;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.platform.Junction;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.ml.encoding.OneHotEncoder;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.ml.OrtMLModel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.List;

public class MLCost implements EstimatableCost {
    public EstimatableCostFactory getFactory() {
        return new Factory();
    }

    public static class Factory implements EstimatableCostFactory {
        @Override public EstimatableCost makeCost() {
            return new MLCost();
        }
    }

    @Override public ProbabilisticDoubleInterval getEstimate(PlanImplementation plan, boolean isOverheadIncluded) {
        try {
            Configuration config = plan
                .getOptimizationContext()
                .getConfiguration();
            OrtMLModel model = OrtMLModel.getInstance(config);

            return ProbabilisticDoubleInterval.ofExactly(
                model.runModel(OneHotEncoder.encode(plan))
            );
        } catch(Exception e) {
            return ProbabilisticDoubleInterval.zero;
        }
    }

    @Override public ProbabilisticDoubleInterval getParallelEstimate(PlanImplementation plan, boolean isOverheadIncluded) {
        try {
            Configuration config = plan
                .getOptimizationContext()
                .getConfiguration();
            OrtMLModel model = OrtMLModel.getInstance(config);

            return ProbabilisticDoubleInterval.ofExactly(
                model.runModel(OneHotEncoder.encode(plan))
            );
        } catch(Exception e) {
            return ProbabilisticDoubleInterval.zero;
        }
    }

    /** Returns a squashed cost estimate. */
    @Override public double getSquashedEstimate(PlanImplementation plan, boolean isOverheadIncluded) {
        try {
            Configuration config = plan
                .getOptimizationContext()
                .getConfiguration();
            OrtMLModel model = OrtMLModel.getInstance(config);

            return model.runModel(OneHotEncoder.encode(plan));
        } catch(Exception e) {
            return 0;
        }
    }

    @Override public double getSquashedParallelEstimate(PlanImplementation plan, boolean isOverheadIncluded) {
        try {
            Configuration config = plan
                .getOptimizationContext()
                .getConfiguration();
            OrtMLModel model = OrtMLModel.getInstance(config);

            return model.runModel(OneHotEncoder.encode(plan));
        } catch(Exception e) {
            return 0;
        }
    }

    @Override public Tuple<List<ProbabilisticDoubleInterval>, List<Double>> getParallelOperatorJunctionAllCostEstimate(PlanImplementation plan, Operator operator) {
        List<ProbabilisticDoubleInterval> intervalList = new ArrayList<ProbabilisticDoubleInterval>();
        List<Double> doubleList = new ArrayList<Double>();
        intervalList.add(this.getEstimate(plan, true));
        doubleList.add(this.getSquashedEstimate(plan, true));

        return new Tuple<>(intervalList, doubleList);
    }

    public PlanImplementation pickBestExecutionPlan(
            Collection<PlanImplementation> executionPlans,
            ExecutionPlan existingPlan,
            Set<Channel> openChannels,
            Set<ExecutionStage> executedStages) {
        final PlanImplementation bestPlanImplementation = executionPlans.stream()
                .reduce((p1, p2) -> {
                    final double t1 = p1.getSquashedCostEstimate();
                    final double t2 = p2.getSquashedCostEstimate();
                    return t1 < t2 ? p1 : p2;
                })
                .orElseThrow(() -> new WayangException("Could not find an execution plan."));
        return bestPlanImplementation;
    }
}
```

Third-party packages such as `OnnxRuntime` can be used to load
pre-trained `.onnx` files that contain desired ML models.

```java
import org.apache.wayang.core.api.Configuration;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.OrtSession.Result;

import java.util.Vector;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.function.BiFunction;

public class OrtMLModel {

    private static OrtMLModel INSTANCE;

    private OrtSession session;
    private OrtEnvironment env;

    private final Map<String, OnnxTensor> inputMap = new HashMap<>();
    private final Set<String> requestedOutputs = new HashSet<>();

    public static OrtMLModel getInstance(Configuration configuration) throws OrtException {
        if (INSTANCE == null) {
            INSTANCE = new OrtMLModel(configuration);
        }

        return INSTANCE;
    }

    private OrtMLModel(Configuration configuration) throws OrtException {
        this.loadModel(configuration.getStringProperty("wayang.ml.model.file"));
    }

    public void loadModel(String filePath) throws OrtException {
        if (this.env == null) {
            this.env = OrtEnvironment.getEnvironment();
        }

        if (this.session == null) {
            this.session = env.createSession(filePath, new OrtSession.SessionOptions());
        }
    }

    public void closeSession() throws OrtException {
        this.session.close();
        this.env.close();
    }

    /**
     * @param encodedVector
     * @return NaN on error, and a predicted cost on any other value.
     * @throws OrtException
     */
    public double runModel(Vector<Long> encodedVector) throws OrtException {
        double costPrediction;

        OnnxTensor tensor = OnnxTensor.createTensor(env, encodedVector);
        this.inputMap.put("input", tensor);
        this.requestedOutputs.add("output");

        BiFunction<Result, String, Double> unwrapFunc = (r, s) -> {
            try {
                return ((double[]) r.get(s).get().getValue())[0];
            } catch (OrtException e) {
                return Double.NaN;
            }
        };

        try (Result r = session.run(inputMap, requestedOutputs)) {
            costPrediction = unwrapFunc.apply(r, "output");
        }

        return costPrediction;
    }
}
```
