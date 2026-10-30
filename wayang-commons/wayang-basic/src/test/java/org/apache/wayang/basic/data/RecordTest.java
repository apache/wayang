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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link Record#compareTo(Record)}.
 */
class RecordTest {

    /**
     * Regression test: the old implementation cast Object[] to Comparable[],
     * which always throws ClassCastException because Object[] is not a
     * subtype of Comparable[].
     */
    @Test
    void compareToShouldNotThrowClassCastExceptionDueToArrayCast() {
        Record r1 = new Record(1, "hello", 3.14);
        Record r2 = new Record(1, "hello", 3.14);

        // This must not throw ClassCastException
        assertEquals(0, r1.compareTo(r2));
    }

    @Test
    void compareToShouldReturnNegativeWhenLessThan() {
        Record r1 = new Record(1, "apple");
        Record r2 = new Record(2, "banana");

        // First field 1 < 2, so r1 < r2
        assert r1.compareTo(r2) < 0;
    }

    @Test
    void compareToShouldReturnPositiveWhenGreaterThan() {
        Record r1 = new Record(2, "banana");
        Record r2 = new Record(1, "apple");

        // First field 2 > 1, so r1 > r2
        assert r1.compareTo(r2) > 0;
    }

    @Test
    void compareToShouldCompareSecondFieldWhenFirstFieldsAreEqual() {
        Record r1 = new Record(1, "apple");
        Record r2 = new Record(1, "banana");

        // First fields equal (1 == 1), second field "apple" < "banana"
        assert r1.compareTo(r2) < 0;
    }

    @Test
    void compareToShouldHandleSingleFieldRecords() {
        Record r1 = new Record(10);
        Record r2 = new Record(20);

        assert r1.compareTo(r2) < 0;
        assert r2.compareTo(r1) > 0;
        assertEquals(0, r1.compareTo(new Record(10)));
    }

    @Test
    void compareToShouldThrowOnDissimilarClasses() {
        Record r1 = new Record(1);
        Record r2 = new Record("one");

        assertThrows(IllegalStateException.class, () -> r1.compareTo(r2));
    }
}
