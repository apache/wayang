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

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

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
        assertNull(stringImplementation.apply(null));
        assertEquals(
                Integer.valueOf(1),
                integerImplementation.apply(new Pojo("testValue", 1))
        );
    }

    @Test
    void testPojoImplementationMultipleFieldsThrows() {
        final ProjectionDescriptor<Pojo, String> multiFieldDescriptor =
                new ProjectionDescriptor<>(Pojo.class, String.class, "string", "integer");
        final Function<Pojo, String> multiFieldImplementation = multiFieldDescriptor.getJavaImplementation();

        assertThrows(
                IllegalStateException.class,
                () -> multiFieldImplementation.apply(new Pojo("testValue", 1))
        );
    }

    @Test
    void testPojoImplementationNonExistentFieldThrows() {
        final ProjectionDescriptor<Pojo, String> invalidDescriptor =
                new ProjectionDescriptor<>(Pojo.class, String.class, "nonExistentField");
        final Function<Pojo, String> invalidImplementation = invalidDescriptor.getJavaImplementation();

        assertThrows(
                IllegalStateException.class,
                () -> invalidImplementation.apply(new Pojo("testValue", 1))
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPojoImplementationSerialization() throws Exception {
        final ProjectionDescriptor<Pojo, String> stringDescriptor =
                new ProjectionDescriptor<>(Pojo.class, String.class, "string");
        Function<Pojo, String> fn = stringDescriptor.getJavaImplementation();

        // Use the function once so that 'field' is populated
        assertEquals("val1", fn.apply(new Pojo("val1", 42)));

        // Serialize and deserialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(fn);
        }

        Function<Pojo, String> deserializedFn;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            deserializedFn = (Function<Pojo, String>) ois.readObject();
        }

        assertNotNull(deserializedFn);
        assertEquals("val2", deserializedFn.apply(new Pojo("val2", 99)));
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

    public static class Pojo implements java.io.Serializable {

        public String string;

        public int integer;

        public Pojo(String string, int integer) {
            this.string = string;
            this.integer = integer;
        }
    }
}
