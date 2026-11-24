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

import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.ExecutionContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.core.util.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * This class executes a stochastic gradient descent optimization on Apache Wayang, just like {@link SGDImpl}. However,
 * it used the {@link org.apache.wayang.basic.operators.MapPartitionsOperator} for performance improvements.
 */
public class SGDImprovedImpl {
    private final Configuration configuration;

    /**
     * {@link Plugin}s to use for the SGD.
     */
    private final List<Plugin> plugins;

    public SGDImprovedImpl(Configuration configuration, Plugin[] plugins) {
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
                .mapPartitions(new TransformPerPartition(features)).withName("transform");


        // Do the SGD
        Collection<double[]> results  =
                weightsBuilder.doWhile(new LoopCondition(accuracy, maxIterations), w -> {
                    // Sample the data and update the weights.
                    DataQuantaBuilder<?, double[]> newWeightsDataset = transformBuilder
                        .sample(sampleSize).withDatasetSize(datasetSize).withBroadcast(w, "weights")
                        .mapPartitions(new ComputeLogisticGradientPerPartition(features)).withBroadcast(w, "weights").withName("compute")
                        .reduce(new Sum()).withName("reduce")
                        .map(new WeightsUpdate()).withBroadcast(w, "weights").withName("update");

                    // Calculate the convergence criterion.
                    DataQuantaBuilder<?, Tuple2<Double, Double>> convergenceDataset = newWeightsDataset
                            .map(new ComputeNorm()).withBroadcast(w, "weights");

                    return new Tuple<>(newWeightsDataset, convergenceDataset);
                }).withExpectedNumberOfIterations(maxIterations).collect();

        // Return the results.
        return WayangCollections.getSingleOrNull(results); // Support null for when execution is skipped.

    }
}

class TransformPerPartition implements FunctionDescriptor.SerializableFunction<Iterable<String>, Iterable<double[]>> {

    int features;

    public TransformPerPartition (int features) {
        this.features = features;
    }

    @Override
    public Iterable<double[]> apply(Iterable<String> lines) {
        List<double[]> list = new ArrayList<>();
        lines.forEach(line -> {
            String[] pointStr = line.split(" ");
            double[] point = new double[features+1];
            point[0] = Double.parseDouble(pointStr[0]);
            for (int i = 1; i < pointStr.length; i++) {
                if (pointStr[i].equals("")) {
                    continue;
                }
                String kv[] = pointStr[i].split(":", 2);
                point[Integer.parseInt(kv[0])-1] = Double.parseDouble(kv[1]);
            }
            list.add(point);
        });
        return list;
    }
}

class ComputeLogisticGradientPerPartition implements FunctionDescriptor.ExtendedSerializableFunction<Iterable<double[]>, Iterable<double[]>> {

    double[] weights;
    double[] sumGradOfPartition;
    int features;

    public ComputeLogisticGradientPerPartition(int features) {
        this.features = features;
        sumGradOfPartition = new double[features + 1]; //position 0 is for the count
    }

    @Override
    public Iterable<double[]> apply(Iterable<double[]> points) {
        List<double[]> list = new ArrayList<>(1);
        points.forEach(point -> {
            double dot = 0;
            for (int j = 0; j < weights.length; j++)
                dot += weights[j] * point[j + 1];
            for (int j = 0; j < weights.length; j++)
                sumGradOfPartition[j + 1] += ((1 / (1 + Math.exp(-1 * dot))) - point[0]) * point[j + 1];

            sumGradOfPartition[0] += 1; //counter for the step size required in the update

        });
        list.add(sumGradOfPartition);
        return list;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.weights = (double[]) executionContext.getBroadcast("weights").iterator().next();
        sumGradOfPartition = new double[features + 1];
    }
}
