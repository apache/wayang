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

package org.apache.wayang.core.util;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Maps keys to multiple values. Each key value pair is unique.
 */
public class MultiMap<K, V> extends LinkedHashMap<K, Set<V>> {

    /**
     * Associate a key with a new value.
     *
     * @param key   to associate with
     * @param value will be associated
     * @return whether the value was not yet associated with the key
     */
    public boolean putSingle(K key, V value) {
        final Set<V> values = this.computeIfAbsent(key, k -> new LinkedHashSet<>());
        return values.add(value);
    }


    /**
     * Disassociate a key with a value.
     *
     * @param key   to disassociate from
     * @param value will be disassociated
     * @return whether the value was associated with the key
     */
    public boolean removeSingle(K key, V value) {
        final Set<V> values = this.get(key);
        if (values != null) {
            return values.remove(value);
        }
        return false;
    }

}
