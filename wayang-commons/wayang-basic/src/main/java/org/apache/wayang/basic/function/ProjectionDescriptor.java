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
    private static class PojoImplementation<Input, Output>
            implements FunctionDescriptor.SerializableFunction<Input, Output> {

        private final String fieldName;

        private Field field;

        private PojoImplementation(final String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Output apply(final Input input) {
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
     * Java implementation of a multi-field projection on POJOs via reflection.
     * Extracts multiple fields by name and packs them into a {@link Record}.
     * {@link Field} objects are resolved lazily on first invocation so the
     * instance stays serializable (only {@code String[]} is captured).
     */
    private static class MultiFieldPojoImplementation<Input>
            implements FunctionDescriptor.SerializableFunction<Input, Record> {

        private final String[] fieldNames;

        private transient Field[] fields;

        private MultiFieldPojoImplementation(final String[] fieldNames) {
            this.fieldNames = fieldNames;
        }

        @Override
        public Record apply(final Input input) {
            if (this.fields == null) {
                this.fields = new Field[this.fieldNames.length];
                final Class<?> typeClass = input.getClass();
                for (int i = 0; i < this.fieldNames.length; i++) {
                    try {
                        this.fields[i] = typeClass.getField(this.fieldNames[i]);
                    } catch (final NoSuchFieldException e) {
                        throw new IllegalStateException(
                                String.format("Could not find field '%s' on %s.", this.fieldNames[i], typeClass), e);
                    }
                }
            }
            final Object[] values = new Object[this.fields.length];
            for (int i = 0; i < this.fields.length; i++) {
                try {
                    values[i] = this.fields[i].get(input);
                } catch (final IllegalAccessException e) {
                    throw new RuntimeException(
                            String.format("Could not access field '%s'.", this.fieldNames[i]), e);
                }
            }
            return new Record(values);
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

    /**
     * Creates a new instance that projects multiple POJO fields by name into a {@link Record}.
     *
     * @param inputTypeClass the input POJO class
     * @param fieldNames     names of the public fields to project
     * @param <Input>        the input type
     * @return the new instance
     */
    public static <Input> ProjectionDescriptor<Input, Record> createForPojoByNames(
            final Class<Input> inputTypeClass, final String... fieldNames) {
        if (fieldNames.length == 0) {
            throw new IllegalArgumentException("At least one field name must be provided.");
        }
        final FunctionDescriptor.SerializableFunction<Input, Record> impl = new MultiFieldPojoImplementation<>(fieldNames);
        return new ProjectionDescriptor<>(
                impl,
                Arrays.asList(fieldNames),
                BasicDataUnitType.createBasic(inputTypeClass),
                (BasicDataUnitType<Record>) BasicDataUnitType.createBasic(Record.class));
    }

    /**
     * Creates a new instance that projects POJO fields by their declared index.
     *
     * @param inputTypeClass the input POJO class
     * @param fieldIndexes   indexes of the public fields to project (in declaration order)
     * @param <Input>        the input type
     * @return the new instance
     */
    public static <Input> ProjectionDescriptor<Input, Record> createForPojoByIndexes(
            final Class<Input> inputTypeClass, final int... fieldIndexes) {
        if (fieldIndexes.length == 0) {
            throw new IllegalArgumentException("At least one field index must be provided.");
        }
        final Field[] allFields = inputTypeClass.getFields();
        final String[] names = new String[fieldIndexes.length];
        for (int i = 0; i < fieldIndexes.length; i++) {
            final int idx = fieldIndexes[i];
            if (idx < 0 || idx >= allFields.length) {
                throw new IllegalArgumentException(
                        String.format("Field index %d is out of bounds (0..%d).", idx, allFields.length - 1));
            }
            names[i] = allFields[idx].getName();
        }
        final FunctionDescriptor.SerializableFunction<Input, Record> impl = new MultiFieldPojoImplementation<>(names);
        return new ProjectionDescriptor<>(
                impl,
                Arrays.asList(names),
                BasicDataUnitType.createBasic(inputTypeClass),
                (BasicDataUnitType<Record>) BasicDataUnitType.createBasic(Record.class));
    }

    @SuppressWarnings("unchecked")
    private static <Input, Output> FunctionDescriptor.SerializableFunction<Input, Output> createPojoJavaImplementation(
            final String[] fieldNames, final BasicDataUnitType<Input> inputType) {
        // Get the names of the fields to be projected.
        if (fieldNames.length == 0) {
            throw new IllegalArgumentException("At least one field name must be provided.");
        }
        if (fieldNames.length == 1) {
            return new PojoImplementation<>(fieldNames[0]);
        }
        return (FunctionDescriptor.SerializableFunction<Input, Output>)
                (FunctionDescriptor.SerializableFunction<Input, ?>)
                new MultiFieldPojoImplementation<>(fieldNames);
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
        this(createPojoJavaImplementation(fieldNames, inputType),
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
