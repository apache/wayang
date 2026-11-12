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


import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.wayang.basic.operators.ObjectFileSerialization;
import org.apache.wayang.core.api.exception.WayangException;

/**
 * Implementation of {@link Value} of flink for use in Wayang
 */
public class WayangValue<T> implements Value {
    private static final long serialVersionUID = 1L;

    private static final int HIGH_BIT = 0x1 << 7;

    private static final int HIGH_BIT2 = 0x1 << 13;

    private static final int HIGH_BIT2_MASK = 0x3 << 6;

    private T data;

    private Class<T> type;

    public WayangValue(){
        this.data = null;
        this.type = null;
    }

    public WayangValue(Class<T> type){
        this.data = null;
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    public WayangValue(T element){
        this.data = element;
        if (element != null) {
            this.type = (Class<T>) element.getClass();
        }
    }

    public WayangValue(T element, Class<T> type){
        this.data = element;
        this.type = type;
    }

    public Class<T> getType() {
        return this.type;
    }

    public void setType(Class<T> type) {
        this.type = type;
    }

    @Override
    public void write(DataOutputView dataOutputView) throws IOException {
        byte[] element;
        try {
            element = convertToByte();
        } catch (WayangException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new IOException("Failed to serialize WayangValue payload", e);
        }
        // write the length, variable-length encoded
        int len = element.length;
        while (len >= HIGH_BIT) {
            dataOutputView.write(len | HIGH_BIT);
            len >>>= 7;
        }
        dataOutputView.write(len);
        dataOutputView.write(element);
    }

    @Override
    public void read(DataInputView dataInputView) throws IOException {
        int len = dataInputView.readUnsignedByte();

        if (len >= HIGH_BIT) {
            int shift = 7;
            int curr;
            len = len & 0x7f;
            while ((curr = dataInputView.readUnsignedByte()) >= HIGH_BIT) {
                if (shift >= 28) {
                    throw new IOException("Malformed WayangValue length header");
                }
                len |= (curr & 0x7f) << shift;
                shift += 7;
            }
            len |= curr << shift;
        }

        long maxBytes = ObjectFileSerialization.getMaxBytes();
        if (len < 0 || len > maxBytes) {
            throw new InvalidClassException(
                    "WayangValue payload length " + len + " exceeds allowed bounds [0, " + maxBytes + "]");
        }

        byte[] array = new byte[len];

        dataInputView.readFully(array);

        try {
            this.data = convertToObject(array, this.type);
        } catch (WayangException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new IOException("Failed to deserialize WayangValue payload", e);
        }
    }

    public byte[] convertToByte() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(this.data);
            out.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new WayangException("Failed to serialize WayangValue payload", e);
        }
    }

    public T convertToObject(byte[] array) {
        return convertToObject(array, this.type);
    }

    @SuppressWarnings("unchecked")
    public T convertToObject(byte[] array, Class<?> expectedType) {
        if (array == null || array.length == 0) {
            return null;
        }
        long maxBytes = ObjectFileSerialization.getMaxBytes();
        if (array.length > maxBytes) {
            throw new WayangException(
                    "Failed to deserialize WayangValue payload",
                    new InvalidClassException(
                            "Payload length " + array.length + " exceeds max bytes limit " + maxBytes));
        }
        Class<?> targetType = resolveExpectedType(expectedType);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(array);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            in.setObjectInputFilter(ObjectFileSerialization.createObjectInputFilter(targetType));
            Object result = in.readObject();
            if (targetType != null && !targetType.isPrimitive() && result != null && !targetType.isInstance(result)) {
                throw new InvalidClassException(
                        result.getClass().getName(),
                        "Deserialized payload is not assignable to expected type " + targetType.getName());
            }
            return (T) result;
        } catch (IOException | ClassNotFoundException e) {
            throw new WayangException("Failed to deserialize WayangValue payload", e);
        }
    }

    private Class<?> resolveExpectedType(Class<?> explicitExpectedType) {
        if (explicitExpectedType != null) {
            return explicitExpectedType;
        }
        if (this.type != null) {
            return this.type;
        }
        return resolveConfiguredExpectedType();
    }

    private Class<?> resolveConfiguredExpectedType() {
        String configuredType = ObjectFileSerialization.resolveConfigProperty(
                ObjectFileSerialization.FLINK_EXPECTED_TYPE_PROPERTY,
                ObjectFileSerialization.EXPECTED_TYPE_PROPERTY,
                ObjectFileSerialization.LEGACY_EXPECTED_TYPE_PROPERTY);
        if (configuredType == null) {
            return null;
        }
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            if (cl == null) {
                cl = getClass().getClassLoader();
            }
            return Class.forName(configuredType, false, cl);
        } catch (ClassNotFoundException | LinkageError e) {
            throw new WayangException("Could not load configured expected payload class: " + configuredType, e);
        }
    }

    public String toString(){
        if(this.data == null){
            return "null";
        }
        return this.data.toString();
    }

    public T get(){
        return this.data;
    }
}
