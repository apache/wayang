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
import org.apache.wayang.basic.operators.ObjectFileSerialization;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    public void testFlinkReadWriteRoundtripWithTypedReceiver() throws Exception {
        WayangValue<CustomPayload> source = new WayangValue<>(new CustomPayload("flink-stream"), CustomPayload.class);
        byte[] bytes = writeToBytes(source);

        WayangValue<CustomPayload> fresh = readFromBytes(bytes, new WayangValue<>(CustomPayload.class));
        Assert.assertNotNull(fresh.get());
        Assert.assertEquals("flink-stream", fresh.get().msg);
        Assert.assertEquals(CustomPayload.class, fresh.getType());
    }

    @Test
    public void testFreshNoArgWayangValueRejectsUntrustedCustomPayloadWithHook() throws Exception {
        CustomPayloadWithHook.hookExecuted = false;
        WayangValue<CustomPayloadWithHook> source = new WayangValue<>(new CustomPayloadWithHook("evil"));
        byte[] bytes = writeToBytes(source);

        WayangValue<CustomPayloadWithHook> fresh = new WayangValue<>();
        try {
            readFromBytes(bytes, fresh);
            Assert.fail("Expected InvalidClassException when deserializing untrusted custom class into no-arg WayangValue");
        } catch (InvalidClassException e) {
            // Expected
        }
        Assert.assertFalse("Custom readObject() hook must not execute", CustomPayloadWithHook.hookExecuted);
    }

    @Test
    public void testWayangListValueRejectsUntrustedAndRoundtripsWithAllowlistOrExpectedType() throws Exception {
        WayangListValue list = new WayangListValue();
        list.add(new WayangValue<>(new CustomPayload("item-1")));
        list.add(new WayangValue<>(new CustomPayload("item-2")));

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(bos);
        list.write(outView);
        byte[] listBytes = bos.toByteArray();

        // Untrusted custom elements must be rejected when no allowlist / expected type is configured
        try {
            readListFromBytes(listBytes);
            Assert.fail("Expected InvalidClassException when deserializing unconfigured custom payloads in WayangListValue");
        } catch (InvalidClassException e) {
            // Expected
        }

        // Trusted allowlist enables deserialization in fresh no-arg WayangValue instances created by WayangListValue
        System.setProperty(ObjectFileSerialization.FLINK_ALLOWLIST_PROPERTY, CustomPayload.class.getName());
        try {
            WayangListValue freshList = readListFromBytes(listBytes);
            Assert.assertEquals(2, freshList.size());
            Assert.assertEquals("item-1", ((CustomPayload) freshList.get(0).get()).msg);
            Assert.assertEquals("item-2", ((CustomPayload) freshList.get(1).get()).msg);
        } finally {
            System.clearProperty(ObjectFileSerialization.FLINK_ALLOWLIST_PROPERTY);
        }

        // Trusted expected-type in Wayang Configuration also enables WayangListValue roundtrip
        Configuration cfg = Configuration.getDefaultConfiguration();
        String prev = cfg.getOptionalStringProperty(ObjectFileSerialization.FLINK_EXPECTED_TYPE_PROPERTY).orElse("");
        cfg.setProperty(ObjectFileSerialization.FLINK_EXPECTED_TYPE_PROPERTY, CustomPayload.class.getName());
        try {
            WayangListValue freshList = readListFromBytes(listBytes);
            Assert.assertEquals(2, freshList.size());
            Assert.assertEquals("item-1", ((CustomPayload) freshList.get(0).get()).msg);
            Assert.assertEquals("item-2", ((CustomPayload) freshList.get(1).get()).msg);
        } finally {
            cfg.setProperty(ObjectFileSerialization.FLINK_EXPECTED_TYPE_PROPERTY, prev);
        }
    }

    @Test
    public void testFreshNoArgWayangValueRejectsCustomNumberSubclass() throws Exception {
        CustomNumberWithHook.hookExecuted = false;
        WayangValue<CustomNumberWithHook> source = new WayangValue<>(new CustomNumberWithHook(42));
        byte[] bytes = writeToBytes(source);

        WayangValue<CustomNumberWithHook> fresh = new WayangValue<>();
        try {
            readFromBytes(bytes, fresh);
            Assert.fail("Expected IOException / InvalidClassException when deserializing CustomNumberWithHook");
        } catch (IOException e) {
            // Expected
        }
        Assert.assertFalse("Custom Number readObject() hook must not execute", CustomNumberWithHook.hookExecuted);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testObjectAndListDeclaredPayloadsRoundtripAndRejectMismatchOrHooks() throws Exception {
        WayangValue<Object> objSource = new WayangValue<>("hello-object", Object.class);
        byte[] objBytes = writeToBytes(objSource);
        Assert.assertEquals("hello-object", readFromBytes(objBytes, new WayangValue<>(Object.class)).get());
        Assert.assertEquals("hello-object", readFromBytes(objBytes, new WayangValue<>()).get());

        List<String> arraysList = Arrays.asList("a", "b");
        byte[] arraysListBytes = writeToBytes(new WayangValue<>(arraysList, (Class) List.class));
        Assert.assertEquals(arraysList, readFromBytes(arraysListBytes, new WayangValue<>((Class) List.class)).get());
        Assert.assertEquals(arraysList, readFromBytes(arraysListBytes, new WayangValue<>()).get());

        List<String> immutableList = List.of("x", "y");
        byte[] immutableListBytes = writeToBytes(new WayangValue<>(immutableList, (Class) List.class));
        Assert.assertEquals(immutableList, readFromBytes(immutableListBytes, new WayangValue<>((Class) List.class)).get());

        ArrayList<String> arrayList = new ArrayList<>(Arrays.asList("p", "q"));
        byte[] arrayListBytes = writeToBytes(new WayangValue<>(arrayList, (Class) List.class));
        Assert.assertEquals(arrayList, readFromBytes(arrayListBytes, new WayangValue<>((Class) List.class)).get());

        // Configured expected-type List.class or Object.class works cleanly on no-arg WayangValue
        Configuration cfg = Configuration.getDefaultConfiguration();
        String prev = cfg.getOptionalStringProperty(ObjectFileSerialization.FLINK_EXPECTED_TYPE_PROPERTY).orElse("");
        cfg.setProperty(ObjectFileSerialization.FLINK_EXPECTED_TYPE_PROPERTY, List.class.getName());
        try {
            Assert.assertEquals(arraysList, readFromBytes(arraysListBytes, new WayangValue<>()).get());
            try {
                readFromBytes(objBytes, new WayangValue<>());
                Assert.fail("Expected InvalidClassException when actual String is not assignable to List.class");
            } catch (InvalidClassException e) {
                // Expected
            }
        } finally {
            cfg.setProperty(ObjectFileSerialization.FLINK_EXPECTED_TYPE_PROPERTY, prev);
        }

        // Broad Object.class / List.class typed receivers must still reject custom hooks
        CustomListWithHook.hookExecuted = false;
        byte[] badListBytes = writeToBytes(new WayangValue<>(new CustomListWithHook(), (Class) List.class));
        try {
            readFromBytes(badListBytes, new WayangValue<>((Class) List.class));
            Assert.fail("Expected InvalidClassException for CustomListWithHook under List.class");
        } catch (InvalidClassException e) {
            // Expected
        }
        Assert.assertFalse("CustomListWithHook readObject() hook must not execute", CustomListWithHook.hookExecuted);
    }

    @Test
    public void testFreshNoArgWayangValueResolvesTrustedConfigAndRejectsOtherCustomClasses() throws Exception {
        byte[] customBytes = writeToBytes(new WayangValue<>(new CustomPayload("trusted-cfg")));
        byte[] hookBytes = writeToBytes(new WayangValue<>(new CustomPayloadWithHook("untrusted-hook")));

        Configuration cfg = Configuration.getDefaultConfiguration();
        String prev = cfg.getOptionalStringProperty(ObjectFileSerialization.FLINK_EXPECTED_TYPE_PROPERTY).orElse("");
        cfg.setProperty(ObjectFileSerialization.FLINK_EXPECTED_TYPE_PROPERTY, CustomPayload.class.getName());
        try {
            CustomPayloadWithHook.hookExecuted = false;
            WayangValue<CustomPayload> fresh = readFromBytes(customBytes, new WayangValue<>());
            Assert.assertEquals("trusted-cfg", fresh.get().msg);

            try {
                readFromBytes(hookBytes, new WayangValue<>());
                Assert.fail("Expected InvalidClassException for unconfigured CustomPayloadWithHook");
            } catch (InvalidClassException e) {
                // Expected
            }
            Assert.assertFalse("Unconfigured hook class must not execute", CustomPayloadWithHook.hookExecuted);
        } finally {
            cfg.setProperty(ObjectFileSerialization.FLINK_EXPECTED_TYPE_PROPERTY, prev);
        }

        System.setProperty(ObjectFileSerialization.ALLOWLIST_PROPERTY, CustomPayload.class.getName());
        try {
            CustomPayloadWithHook.hookExecuted = false;
            WayangValue<CustomPayload> fresh = readFromBytes(customBytes, new WayangValue<>());
            Assert.assertEquals("trusted-cfg", fresh.get().msg);

            try {
                readFromBytes(hookBytes, new WayangValue<>());
                Assert.fail("Expected InvalidClassException for non-allowlisted CustomPayloadWithHook");
            } catch (InvalidClassException e) {
                // Expected
            }
            Assert.assertFalse("Non-allowlisted hook class must not execute", CustomPayloadWithHook.hookExecuted);
        } finally {
            System.clearProperty(ObjectFileSerialization.ALLOWLIST_PROPERTY);
        }
    }

    @Test
    public void testWayangValueEnforcesLengthBoundsAndResourceLimits() throws Exception {
        // Oversized varint length header (Integer.MAX_VALUE) must fail before array allocation
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(bos);
        int len = Integer.MAX_VALUE;
        while (len >= 128) {
            outView.write(len | 128);
            len >>>= 7;
        }
        outView.write(len);
        outView.flush();
        try {
            readFromBytes(bos.toByteArray(), new WayangValue<>());
            Assert.fail("Expected InvalidClassException for oversized WayangValue length header");
        } catch (InvalidClassException e) {
            // Expected
        }

        // Depth limit
        ArrayList<Object> deepList = new ArrayList<>();
        ArrayList<Object> cursor = deepList;
        for (int i = 0; i < 6; i++) {
            ArrayList<Object> next = new ArrayList<>();
            cursor.add(next);
            cursor = next;
        }
        byte[] deepBytes = writeToBytes(new WayangValue<>(deepList, ArrayList.class));
        System.setProperty(ObjectFileSerialization.MAX_DEPTH_PROPERTY, "4");
        try {
            readFromBytes(deepBytes, new WayangValue<>(ArrayList.class));
            Assert.fail("Expected InvalidClassException when depth limit is exceeded");
        } catch (InvalidClassException e) {
            // Expected
        } finally {
            System.clearProperty(ObjectFileSerialization.MAX_DEPTH_PROPERTY);
        }

        // Array-length limit
        byte[] arrayBytes = writeToBytes(new WayangValue<>(new int[]{1, 2, 3, 4, 5}, int[].class));
        System.setProperty(ObjectFileSerialization.MAX_ARRAY_LENGTH_PROPERTY, "3");
        try {
            readFromBytes(arrayBytes, new WayangValue<>(int[].class));
            Assert.fail("Expected InvalidClassException when array-length limit is exceeded");
        } catch (InvalidClassException e) {
            // Expected
        } finally {
            System.clearProperty(ObjectFileSerialization.MAX_ARRAY_LENGTH_PROPERTY);
        }

        // References limit
        ArrayList<StringBuilder> refList = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            refList.add(new StringBuilder("ref-" + i));
        }
        byte[] refBytes = writeToBytes(new WayangValue<>(refList, ArrayList.class));
        System.setProperty(ObjectFileSerialization.MAX_REFERENCES_PROPERTY, "6");
        try {
            readFromBytes(refBytes, new WayangValue<>(ArrayList.class));
            Assert.fail("Expected InvalidClassException when references limit is exceeded");
        } catch (InvalidClassException e) {
            // Expected
        } finally {
            System.clearProperty(ObjectFileSerialization.MAX_REFERENCES_PROPERTY);
        }
    }

    @Test
    public void testReusedNoArgWayangValueReadsMultipleAllowedTypes() throws Exception {
        byte[] intBytes = writeToBytes(new WayangValue<>(Integer.valueOf(7)));
        byte[] strBytes = writeToBytes(new WayangValue<>("second-record"));

        WayangValue<Object> reused = new WayangValue<>();
        readFromBytes(intBytes, reused);
        Assert.assertEquals(7, reused.get());
        readFromBytes(strBytes, reused);
        Assert.assertEquals("second-record", reused.get());
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

    public static class CustomNumberWithHook extends Number implements Serializable {
        private static final long serialVersionUID = 1L;
        public static boolean hookExecuted = false;
        private final int val;

        public CustomNumberWithHook(int val) {
            this.val = val;
        }

        @Override
        public int intValue() { return val; }
        @Override
        public long longValue() { return val; }
        @Override
        public float floatValue() { return val; }
        @Override
        public double doubleValue() { return val; }

        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            hookExecuted = true;
            in.defaultReadObject();
        }
    }

    public static class CustomPayloadWithHook implements Serializable {
        private static final long serialVersionUID = 1L;
        public static boolean hookExecuted = false;
        final String msg;

        public CustomPayloadWithHook(String msg) {
            this.msg = msg;
        }

        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            hookExecuted = true;
            in.defaultReadObject();
        }
    }

    public static class CustomListWithHook extends ArrayList<String> implements Serializable {
        private static final long serialVersionUID = 1L;
        public static boolean hookExecuted = false;

        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            hookExecuted = true;
            in.defaultReadObject();
        }
    }

    private static byte[] writeToBytes(WayangValue<?> value) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(bos);
        value.write(outView);
        return bos.toByteArray();
    }

    private static <T> WayangValue<T> readFromBytes(byte[] bytes, WayangValue<T> target) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(bis);
        target.read(inView);
        return target;
    }

    private static WayangListValue readListFromBytes(byte[] bytes) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(bis);
        WayangListValue list = new WayangListValue();
        list.read(inView);
        return list;
    }
}
