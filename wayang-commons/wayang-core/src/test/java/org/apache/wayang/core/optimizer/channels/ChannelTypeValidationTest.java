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

package org.apache.wayang.core.optimizer.channels;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.test.DummyExecutionOperator;
import org.apache.wayang.core.test.DummyReusableChannel;
import org.apache.wayang.core.types.DataSetType;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChannelTypeValidationTest {

    static class TypedDummyOperator extends DummyExecutionOperator {
        TypedDummyOperator(DataSetType<?> inputType, DataSetType<?> outputType) {
            super(inputType != null ? 1 : 0, outputType != null ? 1 : 0, false);
            if (inputType != null) {
                this.inputSlots[0] = new org.apache.wayang.core.plan.wayangplan.InputSlot<>("in", this, inputType);
            }
            if (outputType != null) {
                this.outputSlots[0] = new org.apache.wayang.core.plan.wayangplan.OutputSlot<>("out", this, outputType);
            }
        }
    }

    @Test
    void testChannelAddConsumerThrowsOnIncompatibleTypes() {
        TypedDummyOperator producerOp = new TypedDummyOperator(null, DataSetType.createDefault(String.class));
        ExecutionTask producerTask = new ExecutionTask(producerOp);
        Channel channel = new DummyReusableChannel(DummyReusableChannel.DESCRIPTOR, producerOp.getOutput(0));
        producerTask.setOutputChannel(0, channel);

        TypedDummyOperator consumerOp = new TypedDummyOperator(DataSetType.createDefault(Integer.class), null);
        ExecutionTask consumerTask = new ExecutionTask(consumerOp);

        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> channel.addConsumer(consumerTask, 0)
        );
        assertTrue(thrown.getMessage().contains("mismatching types"));
    }

    @Test
    void testChannelAddConsumerSucceedsOnCompatibleTypes() {
        TypedDummyOperator producerOp = new TypedDummyOperator(null, DataSetType.createDefault(String.class));
        ExecutionTask producerTask = new ExecutionTask(producerOp);
        Channel channel = new DummyReusableChannel(DummyReusableChannel.DESCRIPTOR, producerOp.getOutput(0));
        producerTask.setOutputChannel(0, channel);

        TypedDummyOperator consumerOp = new TypedDummyOperator(DataSetType.createDefault(CharSequence.class), null);
        ExecutionTask consumerTask = new ExecutionTask(consumerOp);

        assertDoesNotThrow(() -> channel.addConsumer(consumerTask, 0));
    }

    @Test
    void testDefaultChannelConversionThrowsOnIncompatibleTypes() {
        TypedDummyOperator producerOp = new TypedDummyOperator(null, DataSetType.createDefault(String.class));
        Channel channel = new DummyReusableChannel(DummyReusableChannel.DESCRIPTOR, producerOp.getOutput(0));

        DefaultChannelConversion conversion = new DefaultChannelConversion(
                DummyReusableChannel.DESCRIPTOR,
                DummyReusableChannel.DESCRIPTOR,
                (ch, conf) -> new TypedDummyOperator(
                        DataSetType.createDefault(Integer.class),
                        DataSetType.createDefault(Integer.class)
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> conversion.convert(channel, new Configuration(), Collections.emptyList(), null)
        );
    }
}
