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

package org.apache.wayang.basic.operators;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link TableSource}.
 */
class TableSourceTest {

    @Test
    void keepsColumnNames() {
        TableSource source = new TableSource("customer", "name", "age");
        assertArrayEquals(new String[]{"name", "age"}, source.getColumnNames());
    }

    @Test
    void copyKeepsColumnNames() {
        TableSource source = new TableSource("customer", "name", "age");
        TableSource copy = new TableSource(source);
        assertArrayEquals(new String[]{"name", "age"}, copy.getColumnNames());
        copy.getColumnNames()[0] = "changed";
        assertArrayEquals(new String[]{"name", "age"}, source.getColumnNames());
    }

    @Test
    void omitsColumnNamesWhenUnknown() {
        TableSource source = new TableSource("customer");
        assertEquals(0, source.getColumnNames().length);
    }
}
