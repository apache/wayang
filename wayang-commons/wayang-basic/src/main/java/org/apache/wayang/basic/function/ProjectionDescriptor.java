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

package org.apache.wayang.basic.function;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.types.BasicDataUnitType;

/**
 * This descriptor pertains to projections. It takes field names of the input
 * type to describe the projection.
 */
public class ProjectionDescriptor<Input, Output> extends TransformationDescriptor<Input, Output> {

    /**
     * Java implementation of a projection on POJOs via reflection.
     */
    // TODO: Revise implementation to support multiple field projection, by names
    // and indexes.
    private static class PojoImplementation<Input, Output>
            implements FunctionDescriptor.SerializableFunction<Input, Output> {

        private final String fieldName;

        private transient Field field;

        private PojoImplementation(final String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Output apply(final Input input) {
            if (input == null) {
                return null;
            }

            // Initialization code.
            if (this.field == null) {

                // Get the input class.
                final Class<?> typeClass = input.getClass();

                // Find the projection field via reflection.
                try {
                    this.field = typeClass.getField(this.fieldName);
                } catch (final Exception e) {
                    throw new IllegalStateException("The configuration of the projection seems to be illegal.", e);
                }
            }

            // Actual function.
            try {
                return (Output) this.field.get(input);
            } catch (final IllegalAccessException e) {
                throw new RuntimeException("Illegal projection function.", e);
            }
        }
    }

    /**
     * Java implementation of a projection on {@link Record}s.
     */
    private static class RecordImplementation implements FunctionDescriptor.SerializableFunction<Record, Record> {

        /**
         * Indices of the fields to be projected.
         */
        private final int[] fieldIndices;

        /**
         * Creates a new instance.
         *
         * @param recordType {@link RecordType} of input {@link Record}s
         * @param fieldNames that should be projected on
         */
        private RecordImplementation(final RecordType recordType, final String... fieldNames) {
            this.fieldIndices = toIndices(recordType, fieldNames);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Record apply(final Record input) {
            final Object[] projectedFields = new Object[this.fieldIndices.length];
            for (int i = 0; i < this.fieldIndices.length; i++) {
                final int fieldIndex = this.fieldIndices[i];
                projectedFields[i] = input.getField(fieldIndex);
            }
            return new Record(projectedFields);
        }
    }

    /**
     * Creates a new instance that specifically projects {@link Record}s.
     *
     * @param inputType  input {@link RecordType}
     * @param fieldNames names of fields to be projected
     * @return the new instance
     */
    public static ProjectionDescriptor<Record, Record> createForRecords(final RecordType inputType, final String... fieldNames) {
        final SerializableFunction<Record, Record> javaImplementation = createRecordJavaImplementation(fieldNames,
                inputType);
        return new ProjectionDescriptor<>(
                javaImplementation,
                Arrays.asList(fieldNames),
                inputType,
                new RecordType(fieldNames));
    }

    private static <Input, Output> FunctionDescriptor.SerializableFunction<Input, Output> createPojoJavaImplementation(
            final String[] fieldNames) {
        // Get the names of the fields to be projected.
        if (fieldNames.length != 1) {
            return t -> {
                throw new IllegalStateException("The projection descriptor currently supports only a single field.");
            };
        }
        final String fieldName = fieldNames[0];
        return new PojoImplementation<>(fieldName);
    }

    private static FunctionDescriptor.SerializableFunction<Record, Record> createRecordJavaImplementation(
            final String[] fieldNames, final RecordType inputType) {
        return new RecordImplementation(inputType, fieldNames);
    }

    /**
     * Transforms an array of {@link RecordType} field names to indices.
     *
     * @param recordType that maps field names to indices
     * @param fieldNames the field names
     * @return the field indices
     */
    private static int[] toIndices(final RecordType recordType, final String[] fieldNames) {
        final int[] fieldIndices = new int[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            final String fieldName = fieldNames[i];
            fieldIndices[i] = recordType.getIndex(fieldName);
        }
        return fieldIndices;
    }

    private final List<String> fieldNames;

    /**
     * Creates a new instance.
     *
     * @param inputTypeClass  input type
     * @param outputTypeClass output type
     * @param fieldNames      names of the fields to be projected
     */
    public ProjectionDescriptor(final Class<Input> inputTypeClass,
            final Class<Output> outputTypeClass,
            final String... fieldNames) {
        this(BasicDataUnitType.createBasic(inputTypeClass),
                BasicDataUnitType.createBasic(outputTypeClass),
                fieldNames);
    }

    /**
     * Creates a new instance.
     *
     * @param inputType  input type
     * @param outputType output type
     * @param fieldNames names of the fields to be projected
     */
    public ProjectionDescriptor(final BasicDataUnitType<Input> inputType, final BasicDataUnitType<Output> outputType,
            final String... fieldNames) {
        this(createPojoJavaImplementation(fieldNames),
                Collections.unmodifiableList(Arrays.asList(fieldNames)),
                inputType,
                outputType);
    }

    /**
     * Basic constructor.
     *
     * @param javaImplementation Java-based implementation of the projection
     * @param fieldNames         names of the fields to be projected
     * @param inputType          input {@link BasicDataUnitType}
     * @param outputType         output {@link BasicDataUnitType}
     */
    public ProjectionDescriptor(final SerializableFunction<Input, Output> javaImplementation,
            final List<String> fieldNames,
            final BasicDataUnitType<Input> inputType,
            final BasicDataUnitType<Output> outputType) {
        super(javaImplementation, inputType, outputType);
        this.fieldNames = fieldNames;
    }

    public List<String> getFieldNames() {
        return this.fieldNames;
    }
}
