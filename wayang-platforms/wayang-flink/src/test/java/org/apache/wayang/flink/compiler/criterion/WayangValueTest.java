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

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
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
    public void testRejectsDisallowedType() throws Exception {
        AtomicBoolean disallowed = new AtomicBoolean(true);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(disallowed);
            out.flush();
        }

        WayangValue<String> target = new WayangValue<>();
        // convertToObject catches InvalidClassException and returns null
        Object result = target.convertToObject(bos.toByteArray());
        Assert.assertNull(result);
    }
}
