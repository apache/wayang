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
        String typeName = this.type != null ? this.type.getName() : (this.data != null ? this.data.getClass().getName() : null);
        if (typeName != null) {
            dataOutputView.writeBoolean(true);
            dataOutputView.writeUTF(typeName);
        } else {
            dataOutputView.writeBoolean(false);
        }

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
        boolean hasType = dataInputView.readBoolean();
        Class<?> resolvedType = this.type;
        if (hasType) {
            String typeName = dataInputView.readUTF();
            try {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                if (cl == null) {
                    cl = getClass().getClassLoader();
                }
                @SuppressWarnings("unchecked")
                Class<T> loaded = (Class<T>) Class.forName(typeName, false, cl);
                if (this.type != null && !this.type.isAssignableFrom(loaded)) {
                    throw new IOException("Stream payload type " + typeName + " is not compatible with expected type " + this.type.getName());
                }
                if (this.type == null) {
                    this.type = loaded;
                }
                resolvedType = this.type;
            } catch (ClassNotFoundException e) {
                throw new IOException("Could not load payload class: " + typeName, e);
            }
        }

        int len = dataInputView.readUnsignedByte();

        if (len >= HIGH_BIT) {
            int shift = 7;
            int curr;
            len = len & 0x7f;
            while ((curr = dataInputView.readUnsignedByte()) >= HIGH_BIT) {
                len |= (curr & 0x7f) << shift;
                shift += 7;
            }
            len |= curr << shift;
        }

        byte[] array = new byte[len];

        dataInputView.read(array);

        try {
            this.data = convertToObject(array, resolvedType);
            if (this.type == null && this.data != null) {
                @SuppressWarnings("unchecked")
                Class<T> inferredType = (Class<T>) this.data.getClass();
                this.type = inferredType;
            }
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
        return convertToObject(array, this.type != null ? this.type : (this.data != null ? this.data.getClass() : null));
    }

    @SuppressWarnings("unchecked")
    public T convertToObject(byte[] array, Class<?> expectedType) {
        if (array == null || array.length == 0) {
            return null;
        }
        Class<?> targetType = expectedType != null ? expectedType : (this.type != null ? this.type : (this.data != null ? this.data.getClass() : null));
        try (ByteArrayInputStream bis = new ByteArrayInputStream(array);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            in.setObjectInputFilter(ObjectFileSerialization.createObjectInputFilter(targetType));
            return (T) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new WayangException("Failed to deserialize WayangValue payload", e);
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
