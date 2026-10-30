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

package org.apache.wayang.flink.compiler.criterion;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.wayang.core.api.exception.WayangException;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

public class WayangValueTest {

    @Test
    public void testRoundtripAllowedType() {
        WayangValue<String> value = new WayangValue<>("hello");
        byte[] bytes = value.convertToByte();
        Assert.assertNotNull(bytes);

        WayangValue<String> target = new WayangValue<>();
        String deserialized = target.convertToObject(bytes);
        Assert.assertEquals("hello", deserialized);
    }

    @Test
    public void testFreshWayangValueWithCustomPayloadTypeConstructor() {
        WayangValue<CustomPayload> source = new WayangValue<>(new CustomPayload("custom-constructor"));
        byte[] bytes = source.convertToByte();
        Assert.assertNotNull(bytes);

        WayangValue<CustomPayload> fresh = new WayangValue<>(CustomPayload.class);
        CustomPayload deserialized = fresh.convertToObject(bytes);
        Assert.assertNotNull(deserialized);
        Assert.assertEquals("custom-constructor", deserialized.msg);
    }

    @Test
    public void testFreshWayangValueWithSetType() {
        WayangValue<CustomPayload> source = new WayangValue<>(new CustomPayload("custom-setter"));
        byte[] bytes = source.convertToByte();
        Assert.assertNotNull(bytes);

        WayangValue<CustomPayload> fresh = new WayangValue<>();
        fresh.setType(CustomPayload.class);
        CustomPayload deserialized = fresh.convertToObject(bytes);
        Assert.assertNotNull(deserialized);
        Assert.assertEquals("custom-setter", deserialized.msg);
    }

    @Test
    public void testFreshWayangValueWithExplicitExpectedTypeInConvert() {
        WayangValue<CustomPayload> source = new WayangValue<>(new CustomPayload("custom-explicit"));
        byte[] bytes = source.convertToByte();
        Assert.assertNotNull(bytes);

        WayangValue<CustomPayload> fresh = new WayangValue<>();
        CustomPayload deserialized = fresh.convertToObject(bytes, CustomPayload.class);
        Assert.assertNotNull(deserialized);
        Assert.assertEquals("custom-explicit", deserialized.msg);
    }

    @Test
    public void testFlinkReadWriteRoundtripWithCustomPayload() throws Exception {
        WayangValue<CustomPayload> source = new WayangValue<>(new CustomPayload("flink-stream"), CustomPayload.class);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(bos);
        source.write(outView);

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(bis);
        WayangValue<CustomPayload> fresh = new WayangValue<>();
        fresh.read(inView);
        Assert.assertNotNull(fresh.get());
        Assert.assertEquals("flink-stream", fresh.get().msg);
        Assert.assertEquals(CustomPayload.class, fresh.getType());
    }

    @Test
    public void testFreshNoArgWayangValueDeserializesCustomPayload() throws Exception {
        WayangValue<CustomPayload> source = new WayangValue<>(new CustomPayload("fresh-no-arg"));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(bos);
        source.write(outView);

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(bis);
        WayangValue<CustomPayload> fresh = new WayangValue<>();
        fresh.read(inView);
        Assert.assertNotNull(fresh.get());
        Assert.assertEquals("fresh-no-arg", fresh.get().msg);
        Assert.assertEquals(CustomPayload.class, fresh.getType());
    }

    @Test
    public void testRejectsDisallowedTypeThrowsException() throws Exception {
        AtomicBoolean disallowed = new AtomicBoolean(true);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(disallowed);
            out.flush();
        }

        WayangValue<String> target = new WayangValue<>();
        try {
            target.convertToObject(bos.toByteArray());
            Assert.fail("Expected WayangException due to disallowed class");
        } catch (WayangException e) {
            Assert.assertTrue(e.getCause() instanceof InvalidClassException);
        }
    }

    public static class CustomPayload implements Serializable {
        private static final long serialVersionUID = 1L;
        final String msg;

        public CustomPayload(String msg) {
            this.msg = msg;
        }
    }
}
