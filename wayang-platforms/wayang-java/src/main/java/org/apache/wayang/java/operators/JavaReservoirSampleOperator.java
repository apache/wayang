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

package org.apache.wayang.java.operators;

import org.apache.wayang.basic.operators.SampleOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

/**
 * Java implementation of the {@link JavaReservoirSampleOperator}.
 */
public class JavaReservoirSampleOperator<Type>
        extends SampleOperator<Type>
        implements JavaExecutionOperator {

    private Random rand;

    /**
     * Creates a new instance.
     */
    public JavaReservoirSampleOperator(FunctionDescriptor.SerializableIntUnaryOperator sampleSizeFunction, DataSetType<Type> type, FunctionDescriptor.SerializableLongUnaryOperator seed) {
        super(sampleSizeFunction, type, Methods.RESERVOIR, seed);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaReservoirSampleOperator(SampleOperator<Type> that) {
        super(that);
        assert that.getSampleMethod() == Methods.RESERVOIR || that.getSampleMethod() == Methods.ANY;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        int sampleSize = this.getSampleSize(operatorContext);

        if (this.isDataSetSizeKnown() && sampleSize >= datasetSize) { //return all
            ((CollectionChannel.Instance) outputs[0]).accept(((JavaChannelInstance) inputs[0]).provideStream().collect(Collectors.toList()));
        }
        else {
            long seed = this.getSeed(operatorContext);
            rand = new Random(seed);

            ((CollectionChannel.Instance) outputs[0]).accept(reservoirSample(rand, ((JavaChannelInstance) inputs[0]).<Type>provideStream().iterator(), sampleSize));
        }
        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    private static <T> List<T> reservoirSample(Random rand, Iterator<T> items, long m) {
        ArrayList<T> res = new ArrayList<T>(Math.toIntExact(m));
        int count = 0;
        while (items.hasNext()) {
            T item = items.next();
            count++;
            if (count <= m)
                res.add(item);
            else {
                int r = rand.nextInt(count);
                if (r < m)
                    res.set(r, item);
            }
        }
        return res;
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Collections.singleton("wayang.java.reservoir-sample.load");
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaReservoirSampleOperator<>(this);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

}
