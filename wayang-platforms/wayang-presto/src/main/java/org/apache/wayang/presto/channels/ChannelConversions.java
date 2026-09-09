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

package org.apache.wayang.presto.channels;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.optimizer.channels.DefaultChannelConversion;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.jdbc.operators.SqlToRddOperator;
import org.apache.wayang.jdbc.operators.SqlToStreamOperator;
import org.apache.wayang.presto.platform.PrestoPlatform;
import org.apache.wayang.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.Collection;

/**
 * {@link ChannelConversion}s that materialise a Presto SQL query result into a
 * Java {@link StreamChannel} or a Spark {@link RddChannel}.
 */
public class ChannelConversions {

    public static final ChannelConversion SQL_TO_STREAM_CONVERSION = new DefaultChannelConversion(
            PrestoPlatform.getInstance().getSqlQueryChannelDescriptor(),
            StreamChannel.DESCRIPTOR,
            (channel, conf) -> new SqlToStreamOperator<>(
                    PrestoPlatform.getInstance(),
                    channel != null && channel.getProducerSlot() != null ?
                            channel.getProducerSlot().getType() :
                            DataSetType.createDefault(Record.class)
            )
    );

    public static final ChannelConversion SQL_TO_UNCACHED_RDD_CONVERSION = new DefaultChannelConversion(
            PrestoPlatform.getInstance().getSqlQueryChannelDescriptor(),
            RddChannel.UNCACHED_DESCRIPTOR,
            (channel, conf) -> new SqlToRddOperator<>(
                    PrestoPlatform.getInstance(),
                    channel != null && channel.getProducerSlot() != null ?
                            channel.getProducerSlot().getType() :
                            DataSetType.createDefault(Record.class)
            )
    );

    public static final Collection<ChannelConversion> ALL = Arrays.asList(
            SQL_TO_STREAM_CONVERSION,
            SQL_TO_UNCACHED_RDD_CONVERSION
    );

}
