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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.types.RecordType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the {@link ProjectionDescriptor}.
 */
class ProjectionDescriptorTest {

    @Test
    void testPojoImplementation() {
        final ProjectionDescriptor<Pojo, String> stringDescriptor = new ProjectionDescriptor<>(Pojo.class, String.class, "string");
        final Function<Pojo, String> stringImplementation = stringDescriptor.getJavaImplementation();

        final ProjectionDescriptor<Pojo, Integer> integerDescriptor = new ProjectionDescriptor<>(Pojo.class, Integer.class, "integer");
        final Function<Pojo, Integer> integerImplementation = integerDescriptor.getJavaImplementation();

        assertEquals(
                "testValue",
                stringImplementation.apply(new Pojo("testValue", 1))
        );
        assertNull(stringImplementation.apply(new Pojo(null, 1)));
        assertEquals(
                Integer.valueOf(1),
                integerImplementation.apply(new Pojo("testValue", 1))
        );

    }

    @Test
    void testRecordImplementation() {
        RecordType inputType = new RecordType("a", "b", "c");
        final ProjectionDescriptor<Record, Record> descriptor = ProjectionDescriptor.createForRecords(inputType, "c", "a");
        assertEquals(new RecordType("c", "a"), descriptor.getOutputType());

        final Function<Record, Record> javaImplementation = descriptor.getJavaImplementation();
        assertEquals(
                new Record("world", 10),
                javaImplementation.apply(new Record(10, "hello", "world"))
        );
    }

    @Test
    void testMultiFieldPojoProjectionByName() {
        final ProjectionDescriptor<Pojo, Record> descriptor =
                ProjectionDescriptor.createForPojoByNames(Pojo.class, "string", "integer");
        final Function<Pojo, Record> implementation = descriptor.getJavaImplementation();

        assertEquals(
                new Record("testValue", 1),
                implementation.apply(new Pojo("testValue", 1))
        );
    }

    @Test
    void testMultiFieldPojoProjectionReordersFields() {
        final ProjectionDescriptor<Pojo, Record> descriptor =
                ProjectionDescriptor.createForPojoByNames(Pojo.class, "integer", "string");
        final Function<Pojo, Record> implementation = descriptor.getJavaImplementation();

        assertEquals(
                new Record(42, "hello"),
                implementation.apply(new Pojo("hello", 42))
        );
    }

    @Test
    void testMultiFieldPojoProjectionWithNulls() {
        final ProjectionDescriptor<Pojo, Record> descriptor =
                ProjectionDescriptor.createForPojoByNames(Pojo.class, "string", "integer");
        final Function<Pojo, Record> implementation = descriptor.getJavaImplementation();

        assertEquals(
                new Record(null, 0),
                implementation.apply(new Pojo(null, 0))
        );
    }

    @Test
    void testPojoProjectionByIndex() {
        // Pojo declares: string (index 0), integer (index 1)
        final ProjectionDescriptor<Pojo, Record> descriptor =
                ProjectionDescriptor.createForPojoByIndexes(Pojo.class, 1, 0);
        final Function<Pojo, Record> implementation = descriptor.getJavaImplementation();

        assertEquals(
                new Record(99, "abc"),
                implementation.apply(new Pojo("abc", 99))
        );
        assertEquals(Arrays.asList("integer", "string"), descriptor.getFieldNames());
    }

    @Test
    void testPojoProjectionByIndexSingleField() {
        // Pojo declares: string (index 0), integer (index 1)
        final ProjectionDescriptor<Pojo, Record> descriptor =
                ProjectionDescriptor.createForPojoByIndexes(Pojo.class, 0);
        final Function<Pojo, Record> implementation = descriptor.getJavaImplementation();

        assertEquals(
                new Record("hello"),
                implementation.apply(new Pojo("hello", 5))
        );
        assertEquals(Arrays.asList("string"), descriptor.getFieldNames());
    }

    @Test
    void testPojoProjectionByIndexOutOfBounds() {
        assertThrows(IllegalArgumentException.class, () ->
                ProjectionDescriptor.createForPojoByIndexes(Pojo.class, 5));
    }

    @Test
    void testPojoProjectionByNameNonexistentField() {
        final ProjectionDescriptor<Pojo, Record> descriptor =
                ProjectionDescriptor.createForPojoByNames(Pojo.class, "string", "nonexistent");
        final Function<Pojo, Record> implementation = descriptor.getJavaImplementation();

        assertThrows(IllegalStateException.class, () ->
                implementation.apply(new Pojo("test", 1)));
    }

    @Test
    void testPojoProjectionEmptyFieldNames() {
        assertThrows(IllegalArgumentException.class, () ->
                new ProjectionDescriptor<>(Pojo.class, Record.class));
    }

    @Test
    void testPojoProjectionByIndexEmpty() {
        assertThrows(IllegalArgumentException.class, () ->
                ProjectionDescriptor.createForPojoByIndexes(Pojo.class));
    }

    public static class Pojo {

        public String string;

        public int integer;

        public Pojo(String string, int integer) {
            this.string = string;
            this.integer = integer;
        }
    }
}
