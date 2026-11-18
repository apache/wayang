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

package org.apache.wayang.api.python.executor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

import org.apache.wayang.core.api.exception.WayangException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReaderIteratorTest {

    @Test
    void readsEmptyAndNonemptyStringsInOrder() throws IOException {
        final String[] values = {"", "hello", "", "雪", ""};
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final DataOutputStream output = new DataOutputStream(bytes);
        for (String value : values) {
            final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
            output.writeInt(encoded.length);
            output.write(encoded);
        }
        output.writeInt(ProcessFeeder.END_OF_DATA_SECTION);

        final ReaderIterator<String> iterator = new ReaderIterator<>(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        for (String value : values) {
            assertTrue(iterator.hasNext());
            assertEquals(value, iterator.next());
        }
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    void endOfDataSectionStopsReading() throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final DataOutputStream output = new DataOutputStream(bytes);
        output.writeInt(ProcessFeeder.END_OF_DATA_SECTION);
        output.writeInt(0);
        final DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        final ReaderIterator<String> iterator = new ReaderIterator<>(input);

        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
        assertEquals(Integer.BYTES, input.available());
    }

    @Test
    void preservesNegativeMarkerHandling() throws IOException {
        // Python SpecialLengths markers other than END_OF_DATA_SECTION, plus an unknown marker.
        // ReaderIterator currently returns null for each without terminating the stream.
        final int[] markers = {-2, -3, -4, ProcessFeeder.NULL, -6, -7};
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final DataOutputStream output = new DataOutputStream(bytes);
        for (int marker : markers) {
            output.writeInt(marker);
        }
        output.writeInt(2);
        output.write("ok".getBytes(StandardCharsets.UTF_8));
        output.writeInt(ProcessFeeder.END_OF_DATA_SECTION);
        final ReaderIterator<String> iterator = new ReaderIterator<>(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        for (int marker : markers) {
            assertTrue(iterator.hasNext(), "marker " + marker);
            assertNull(iterator.next(), "marker " + marker);
        }
        assertTrue(iterator.hasNext());
        assertEquals("ok", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    void wrapsPrematureEndOfStream() {
        // Missing length, truncated length, and truncated payload remain I/O errors.
        final byte[][] truncatedStreams = {{}, {0, 0, 0}, {0, 0, 0, 2, 'a'}};
        for (byte[] bytes : truncatedStreams) {
            final ReaderIterator<String> iterator = new ReaderIterator<>(
                    new DataInputStream(new ByteArrayInputStream(bytes)));
            final WayangException exception = assertThrows(WayangException.class, iterator::hasNext);
            assertTrue(exception.getCause() instanceof EOFException);
        }
    }
}
