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

package org.apache.wayang.api.sql.calcite.converter.functions;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.function.FunctionDescriptor;

/**
 * Flattens {@code Tuple2<Record, Record>} to {@link Record}.
 */
public class MultiConditionJoinFuncImpl implements FunctionDescriptor.SerializableFunction<Tuple2<Record, Record>, Record> {

    /**
     * Creates a new instance.
     */
    public MultiConditionJoinFuncImpl() {

    }

    @Override
    public Record apply(final Tuple2<Record, Record> tuple2) {
        final int length1 = ((Tuple2<Record, Record>) tuple2).getField0().size();
        final int length2 = ((Tuple2<Record, Record>) tuple2).getField1().size();
        
        final int totalLength = length1 + length2;

        final Object[] fields = new Object[totalLength];

        for (int i = 0; i < length1; i++) {
            fields[i] = ((Tuple2<Record, Record>) tuple2).getField0().getField(i);
        }
        for (int j = length1; j < totalLength; j++) {
            fields[j] = ((Tuple2<Record, Record>) tuple2).getField1().getField(j - length1);
        }

        return new Record(fields);
    }
}