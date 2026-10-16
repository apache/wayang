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

package org.apache.wayang.basic.data;

import org.apache.wayang.core.util.Copyable;
import org.apache.wayang.core.util.ReflectionUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A Type that represents a record with a schema, might be replaced with
 * something standard like JPA entity.
 */
public class Record implements Serializable, Copyable<Record>, Comparable<Record> {

    private Object[] values;

    public Object[] getValues() {
        return values;
    }

    public Record(final Object... values) {
        this.values = values;
    }

    public Record(final List<Object> values) {
        this.values = values.toArray();
    }

    @Override
    public Record copy() {
        return new Record(this.values.clone());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || this.getClass() != o.getClass())
            return false;
        final Record record2 = (Record) o;
        return Arrays.equals(this.values, record2.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(values));
    }

    @Override
    public String toString() { return "Record" + Arrays.toString(this.values); }

    public Object getField(final int index) {
        return this.values[index];
    }

    /**
     * Retrieve a field as a {@code double}. It must be castable as such.
     *
     * @param index the index of the field
     * @return the {@code double} representation of the field
     */
    public double getDouble(final int index) {
        final Object field = this.values[index];
        return ReflectionUtils.toDouble(field);
    }

    /**
     * Retrieve a field as a {@code long}. It must be castable as such.
     *
     * @param index the index of the field
     * @return the {@code long} representation of the field
     */
    public long getLong(final int index) {
        final Object field = this.values[index];
        if (field instanceof Integer)
            return (Integer) field;
        else if (field instanceof Long)
            return (Long) field;
        else if (field instanceof Short)
            return (Short) field;
        else if (field instanceof Byte)
            return (Byte) field;
        throw new IllegalStateException(String.format("%s cannot be retrieved as long.", field));
    }

    /**
     * Retrieve a field as a {@code int}. It must be castable as such.
     *
     * @param index the index of the field
     * @return the {@code int} representation of the field
     */
    public int getInt(final int index) {
        final Object field = this.values[index];
        if (field instanceof Integer)
            return (Integer) field;
        else if (field instanceof Short)
            return (Short) field;
        else if (field instanceof Byte)
            return (Byte) field;
        throw new IllegalStateException(String.format("%s cannot be retrieved as int.", field));
    }

    /**
     * Retrieve a field as a {@link String}.
     *
     * @param index the index of the field
     * @return the field as a {@link String} (obtained via
     *         {@link Object#toString()}) or {@code null} if the field is
     *         {@code null}
     */
    public String getString(final int index) {
        final Object field = this.values[index];
        return field == null ? null : field.toString();
    }

    /**
     * Set a field of this instance, at a given index.
     *
     * @param index the index of the field
     * @param field the new value of the field to be set
     */
    public void setField(final int index, final Object field) {
        this.values[index] = field;
    }

    /**
     * Append a field to this instance.
     *
     * @param field the field to add
     */
    public void addField(final Object field) {
        final int size = this.size();
        final Object[] newValues = Arrays.copyOf(this.values, size + 1);
        newValues[size] = field;
        this.values = newValues;
    }

    /**
     * Retrieve the size of this instance.
     *
     * @return the number of fields in this instance
     */
    public int size() {
        return this.values.length;
    }

    /**
     * Compares the fields of this record to the fields of another record.
     * 
     * @param that another record not null
     * @return
     * @throws IllegalStateException if the two records do not have the same types in {@link #values}
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public int compareTo(final Record that) throws IllegalStateException {
        for (int i = 0; i < values.length; i++) {
            if (!this.values[i].getClass().equals(that.values[i].getClass()))
                throw new IllegalStateException("Tried compare records with dissimilar classes had, this values: "
                        + this.values + ", that values: " + that.values + ", this item class: "
                        + this.values[i].getClass() + ", that item class: " + that.values[i].getClass());

            final Comparable thisValue = (Comparable) this.values[i];
            final int comparison = thisValue.compareTo(that.values[i]);
            if (comparison != 0) {
                return comparison;
            }
        }

        return 0;
    }
}
