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

package org.apache.wayang.tensorflow.model;

import org.apache.wayang.basic.model.DLModel;
import org.apache.wayang.basic.model.op.Input;
import org.apache.wayang.basic.model.op.Op;
import org.apache.wayang.basic.model.optimizer.Optimizer;
import org.tensorflow.*;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.ndarray.Shape;
import org.tensorflow.ndarray.index.Indices;
import org.tensorflow.op.Ops;
import org.tensorflow.op.core.Placeholder;
import org.tensorflow.types.TBool;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.family.TType;

import java.util.*;
import java.util.stream.Collectors;

public class TensorflowModel extends DLModel implements AutoCloseable {
    private final Op criterion;
    private final Optimizer optimizer;
    private final Op accuracyCalculation;

    private final Graph graph;
    private final Ops tf;
    private final Placeholder<TBool> trainingMode;
    private final Session session;
    private final Map<Integer, Operand<?>> opMap;
    private final org.tensorflow.op.Op minimize;
    private final List<Result> pendingResults = new ArrayList<>();

    public TensorflowModel(DLModel model, Op criterion, Optimizer optimizer, Op accuracyCalculation) {
        super(model.getOut());
        this.criterion = criterion;
        this.optimizer = optimizer;
        this.accuracyCalculation = accuracyCalculation;

        this.graph = new Graph();
        this.tf = Ops.create(graph);
        this.trainingMode = tf.placeholder(TBool.class, Placeholder.shape(Shape.scalar()));
        this.session = new Session(graph);
        this.opMap = new HashMap<>();

        compile(criterion);
        if (accuracyCalculation != null) {
            compile(accuracyCalculation);
        }

        this.minimize = Convertor.convert(graph, optimizer).minimize(opMap.get(criterion.getId()));
    }

    private Operand<?> compile(Op op) {
        List<Operand<?>> inputs = op.getFromList().stream().map(e -> {
            Operand<?> operand = this.opMap.get(e.getId());
            if (operand == null) {
                operand = compile(e);
            }
            return operand;
        }).collect(Collectors.toList());
        inputs.add(trainingMode);
        final Operand<?> ret = Convertor.convert(graph, tf, op, inputs.toArray(Operand[]::new));
        this.opMap.put(op.getId(), ret);
        return ret;
    }

    public
    <XT extends NdArray<?>, YT extends NdArray<?>>
    void train(XT x, YT y, int epoch, int batchSize) {
        System.out.println("Start training:");

        for (int i = 0; i < epoch; i++) {
            int n = (int) y.shape().size(0);
            for (int start = 0; start < n; start += batchSize) {
                int end = Math.min(start + batchSize, n);
                NdArray<?> x_ = x.slice(Indices.slice(start, end));
                NdArray<?> y_ = y.slice(Indices.slice(start, end));
                try (
                        Tensor tx = Convertor.ndArrayToTensor(x_);
                        Tensor ty = Convertor.ndArrayToTensor(y_);
                        Tensor training = TBool.scalarOf(true);
                ) {
                    Session.Runner runner = session.runner()
                            .feed(Input.Type.FEATURES.getName(), tx)
                            .feed(Input.Type.LABEL.getName(), ty)
                            .feed(trainingMode, training)
                            .addTarget(minimize)
                            .fetch(criterion.getName());
                    if (accuracyCalculation != null) {
                        runner.fetch(accuracyCalculation.getName());
                    }
                    try (Result ret = runner.run()) {
                        TFloat32 loss = (TFloat32) ret.get(0);
                        System.out.printf("[epoch %d, batch %d] loss: %f ", i + 1, start / batchSize + 1, loss.getFloat());

                        if (accuracyCalculation != null) {
                            TFloat32 acc = (TFloat32) ret.get(1);
                            System.out.printf("accuracy: %f ", acc.getFloat());
                        }
                    }
                    System.out.println();
                }
            }
        }

        System.out.println("Finish training.\n");
    }

    public
    <XT extends NdArray<?>, PT extends NdArray<?> & TType>
    PT predict(XT x) {
        try (Tensor tx = Convertor.ndArrayToTensor(x);
             Tensor training = TBool.scalarOf(false)) {
            Result result = session.runner()
                    .feed(Input.Type.FEATURES.getName(), tx)
                    .feed(trainingMode, training)
                    .fetch(out.getName())
                    .run();
            // The result owns the fetched tensor and must be closed together with it.
            // Keep it until the model is closed so that native memory is released
            // deterministically instead of by the garbage collector at JVM shutdown.
            this.pendingResults.add(result);
            return (PT) result.get(0);
        }
    }

    public Op getCriterion() {
        return criterion;
    }

    public Optimizer getOptimizer() {
        return optimizer;
    }

    public Op getAccuracyCalculation() {
        return accuracyCalculation;
    }

    @Override
    public void close() {
        for (Result result : this.pendingResults) {
            result.close();
        }
        this.pendingResults.clear();
        session.close();
        graph.close();
    }
}
