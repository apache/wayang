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

import java.util.Arrays;
import java.util.Objects;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

/**
 * Keeps only the {@link Record}s whose join key fields are all non-null. In SQL, a NULL join key
 * never matches, not even another NULL, so these records can be dropped before the join.
 */
public class JoinKeyNotNullFilter implements FunctionDescriptor.SerializablePredicate<Record> {
    /**
     * Indexes of the fields that make up the key.
     */
    private final Integer[] indexes;

    /**
     * Creates a new instance.
     *
     * @param indexes indexes of the fields that make up the key
     */
    public JoinKeyNotNullFilter(final Integer... indexes) {
        this.indexes = indexes;
    }

    @Override
    public boolean test(final Record record) {
        return Arrays.stream(indexes).map(record::getField).allMatch(Objects::nonNull);
    }
}
