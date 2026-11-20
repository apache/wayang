/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.apps.sgd;

import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.ExecutionContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.core.util.Tuple;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * This class executes a stochastic gradient descent optimization on Apache Wayang.
 */
public class SGDImpl {

    private final Configuration configuration;

    /**
     * {@link Plugin}s to use for the SGD.
     */
    private final List<Plugin> plugins;

    public SGDImpl(Configuration configuration, Plugin[] plugins) {
        this.configuration = configuration;
        this.plugins = Arrays.asList(plugins);
    }

    public double[] apply(String datasetUrl,
                          int datasetSize,
                          int features,
                          int maxIterations,
                          double accuracy,
                          int sampleSize) {
        return this.apply(datasetUrl, datasetSize, features, maxIterations, accuracy, sampleSize, null);
    }

    public double[] apply(String datasetUrl,
                          int datasetSize,
                          int features,
                          int maxIterations,
                          double accuracy,
                          int sampleSize,
                          Experiment experiment) {

        // Initialize the builder.
        WayangContext wayangContext = new WayangContext(this.configuration);
        for (Plugin plugin : this.plugins) {
            wayangContext.withPlugin(plugin);
        }
        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(wayangContext);
        if (experiment != null) javaPlanBuilder.withExperiment(experiment);
        javaPlanBuilder.withUdfJarOf(this.getClass());

        // Create initial weights.
        List<double[]> weights = Arrays.asList(new double[features]);
        final DataQuantaBuilder<?, double[]> weightsBuilder = javaPlanBuilder
                .loadCollection(weights).withName("init weights");

        // Load and transform the data.
        final DataQuantaBuilder<?, double[]> transformBuilder = javaPlanBuilder
                .readTextFile(datasetUrl).withName("source")
                .map(new Transform(features)).withName("transform");

        // Do the SGD
        Collection<double[]> results =
                weightsBuilder.doWhile(new LoopCondition(accuracy, maxIterations), w -> {
                    // Sample the data and update the weights.
                    DataQuantaBuilder<?, double[]> newWeightsDataset = transformBuilder
                            .sample(sampleSize).withDatasetSize(datasetSize).withBroadcast(w, "weights")
                            .map(new ComputeLogisticGradient()).withBroadcast(w, "weights").withName("compute")
                            .reduce(new Sum()).withName("reduce")
                            .map(new WeightsUpdate()).withBroadcast(w, "weights").withName("update");

                    // Calculate the convergence criterion.
                    DataQuantaBuilder<?, Tuple2<Double, Double>> convergenceDataset = newWeightsDataset
                            .map(new ComputeNorm()).withBroadcast(w, "weights");

                    return new Tuple<>(newWeightsDataset, convergenceDataset);
                }).withExpectedNumberOfIterations(maxIterations).collect();

        // Return the results.
        return WayangCollections.getSingleOrNull(results); // null to support experiments with skipped execution
    }
}

class Transform implements FunctionDescriptor.SerializableFunction<String, double[]> {

    int features;

    public Transform(int features) {
        this.features = features;
    }

    @Override
    public double[] apply(String line) {
        String[] pointStr = line.split(",");
        double[] point = new double[features + 1];
        point[0] = Double.parseDouble(pointStr[0]);
        for (int i = 1; i < pointStr.length; i++) {
/*            if (pointStr[i].equals("")) {
                continue;
            }
            String kv[] = pointStr[i].split(":", 2);
            point[Integer.parseInt(kv[0]) - 1] = Double.parseDouble(kv[1]);*/
            point[i] = Double.parseDouble(pointStr[i]);
        }
        return point;
    }
}

class ComputeLogisticGradient implements FunctionDescriptor.ExtendedSerializableFunction<double[], double[]> {

    double[] weights;

    @Override
    public double[] apply(double[] point) {
        double[] gradient = new double[point.length];
        double dot = 0;
        for (int j = 0; j < weights.length; j++)
            dot += weights[j] * point[j + 1];

        for (int j = 0; j < weights.length; j++)
            gradient[j + 1] = ((1 / (1 + Math.exp(-1 * dot))) - point[0]) * point[j + 1];

        gradient[0] = 1; //counter for the step size required in the update

        return gradient;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.weights = (double[]) executionContext.getBroadcast("weights").iterator().next();
    }
}

class Sum implements FunctionDescriptor.SerializableBinaryOperator<double[]> {

    @Override
    public double[] apply(double[] o, double[] o2) {
        double[] g1 = o;
        double[] g2 = o2;

        if (g2 == null) //samples came from one partition only
            return g1;

        if (g1 == null) //samples came from one partition only
            return g2;

        double[] sum = new double[g1.length];
        sum[0] = g1[0] + g2[0]; //count
        for (int i = 1; i < g1.length; i++)
            sum[i] = g1[i] + g2[i];

        return sum;
    }
}

class WeightsUpdate implements FunctionDescriptor.ExtendedSerializableFunction<double[], double[]> {

    double[] weights;
    int current_iteration;

    double stepSize = 1;
    double regulizer = 0;

    public WeightsUpdate() {
    }

    public WeightsUpdate(double stepSize, double regulizer) {
        this.stepSize = stepSize;
        this.regulizer = regulizer;
    }

    @Override
    public double[] apply(double[] input) {

        double count = input[0];
        double alpha = (stepSize / (current_iteration + 1));
        double[] newWeights = new double[weights.length];
        for (int j = 0; j < weights.length; j++) {
            newWeights[j] = (1 - alpha * regulizer) * weights[j] - alpha * (1.0 / count) * input[j + 1];
        }
        return newWeights;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.weights = (double[]) executionContext.getBroadcast("weights").iterator().next();
        this.current_iteration = executionContext.getCurrentIteration();
    }
}

class ComputeNorm implements FunctionDescriptor.ExtendedSerializableFunction<double[], Tuple2<Double, Double>> {

    double[] previousWeights;

    @Override
    public Tuple2<Double, Double> apply(double[] weights) {
        double normDiff = 0.0;
        double normWeights = 0.0;
        for (int j = 0; j < weights.length; j++) {
//            normDiff += Math.sqrt(Math.pow(Math.abs(weights[j] - input[j]), 2));
            normDiff += Math.abs(weights[j] - previousWeights[j]);
//            normWeights += Math.sqrt(Math.pow(Math.abs(input[j]), 2));
            normWeights += Math.abs(weights[j]);
        }
        return new Tuple2(normDiff, normWeights);
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.previousWeights = (double[]) executionContext.getBroadcast("weights").iterator().next();
    }
}


class LoopCondition implements FunctionDescriptor.ExtendedSerializablePredicate<Collection<Tuple2<Double, Double>>> {

    public double accuracy;
    public int max_iterations;

    private int current_iteration;

    public LoopCondition(double accuracy, int max_iterations) {
        this.accuracy = accuracy;
        this.max_iterations = max_iterations;
    }

    @Override
    public boolean test(Collection<Tuple2<Double, Double>> collection) {
        Tuple2<Double, Double> input = WayangCollections.getSingle(collection);
        System.out.println("Running iteration: " + current_iteration);
        return (input.field0 < accuracy * Math.max(input.field1, 1.0) || current_iteration > max_iterations);
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.current_iteration = executionContext.getCurrentIteration();
    }
}
