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

package org.apache.wayang.ml.encoding;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.ml.util.Logging;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.OrtSession.Result;

public class OrtMLModel {
    private static OrtMLModel INSTANCE;

    public static OrtMLModel getInstance(final Configuration configuration) throws OrtException {
        if (INSTANCE == null) {
            INSTANCE = new OrtMLModel(configuration);
        }

        return INSTANCE;
    }

    private OrtSession session;
    private OrtEnvironment env;

    private final Configuration configuration;
    private final Map<String, OnnxTensor> inputMap = new HashMap<>();

    private final Set<String> requestedOutputs = new HashSet<>();

    private OrtMLModel(final Configuration configuration) throws OrtException {
        this.configuration = configuration;
        this.loadModel(configuration.getStringProperty("wayang.ml.model.file"));
    }

    /**
     * placeholder
     * 
     * @param encoded
     * @return
     */
    public double runModel(final long[] encoded) {
        return 0;
    }

    public static void printTupleDeep(Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input) {
        System.out.println("=== VALUES (field0) ===");
        for (int k = 0; k < input.field0.size(); k++) {
            System.out.println("Tree " + k + ":");

            long[][] arr = input.field0.get(k);
            for (int i = 0; i < arr.length; i++) {
                System.out.print("  Node " + i + ": ");
                for (int j = 0; j < arr[i].length; j++) {
                    System.out.print(arr[i][j] + " ");
                }
                System.out.println();
            }
        }

        System.out.println("\n=== INDEXES (field1) ===");
        for (int k = 0; k < input.field1.size(); k++) {
            System.out.println("Tree " + k + ":");

            long[][] arr = input.field1.get(k);
            for (int i = 0; i < arr.length; i++) {
                System.out.print("  Idx " + i + ": ");
                for (int j = 0; j < arr[i].length; j++) {
                    System.out.print(arr[i][j] + " ");
                }
                System.out.println();
            }
        }
    }

    /**
     * Close the session after running, {@link #closeSession()}
     * 
     * @param input1
     * @return NaN on error, and a predicted cost on any other value.
     * @throws OrtException
     */
    public double runModel(final Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input1) throws OrtException {
        final int batchSize = input1.getField0().size();
        final long[][] firstValues = input1.getField0().get(0);
        final int featureSize = firstValues.length;
        final int sequenceLength = firstValues[0].length; 

        final Instant start = Instant.now();
        final float[][][] inputValueStructure = new float[batchSize][featureSize][sequenceLength];
        final long[][][] inputIndexStructure = new long[batchSize][featureSize][sequenceLength];

        for (int i = 0; i < input1.field0.get(0).length; i++) {
            for (int j = 0; j < input1.field0.get(0)[i].length; j++) {
                inputValueStructure[0][i][j] = Long.valueOf(input1.field0.get(0)[i][j]).floatValue();
            }
        }

        for (int i = 0; i < input1.field1.get(0).length; i++) {
            inputIndexStructure[0][i] = input1.field1.get(0)[i];
        }

        final OnnxTensor tensorValues = OnnxTensor.createTensor(env, inputValueStructure);
        final OnnxTensor tensorIndexes = OnnxTensor.createTensor(env, inputIndexStructure);

        this.inputMap.put("input1", tensorValues);
        this.inputMap.put("input2", tensorIndexes);

        this.requestedOutputs.add("output");

        final BiFunction<Result, String, Float> unwrapFunc = (r, s) -> {
            try {
                return ((float[]) r.get(s).get().getValue())[0];
            } catch (final OrtException e) {
                this.inputMap.clear();
                this.requestedOutputs.clear();

                return Float.NaN;
            }
        };

        final double costPrediction;

        try (Result r = session.run(inputMap, requestedOutputs)) {
            costPrediction = unwrapFunc.apply(r, "output");
            final Instant end = Instant.now();
            final long execTime = Duration.between(start, end).toMillis();

            Logging.writeToFile(String.format("%d", execTime),
                    this.configuration.getStringProperty("wayang.ml.optimizations.file"));
        } catch (final Exception e) {
            e.printStackTrace();
            return 0;
        } finally {
            this.inputMap.clear();
            this.requestedOutputs.clear();
        }

        return costPrediction;
    }
    
    /**
     * Closes the OrtModel resource, relinquishing any underlying resources.
     * 
     * @throws OrtException
     */
    public void closeSession() throws OrtException {
        this.session.close();
        this.env.close();
    }

    private void loadModel(final String filePath) throws OrtException {
        if (this.env == null) {
            this.env = OrtEnvironment.getEnvironment("org.apache.wayang.ml");
            this.env.setTelemetry(false);
        }

        if (this.session == null) {
            final OrtSession.SessionOptions options = new OrtSession.SessionOptions();

            options.setInterOpNumThreads(16);
            options.setIntraOpNumThreads(16);
            options.setDeterministicCompute(true);

            this.session = env.createSession(filePath, options);
        }
    }
}
