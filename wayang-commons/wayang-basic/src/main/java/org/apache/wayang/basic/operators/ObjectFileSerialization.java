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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Currency;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Utility methods that convert between Java objects and the on-disk representation used by {@link ObjectFileSink}.
 */
public final class ObjectFileSerialization {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    static {
        OBJECT_MAPPER.findAndRegisterModules();
    }

    private ObjectFileSerialization() {
    }

    /**
     * Serialize a chunk of objects using the provided {@link ObjectFileSerializationMode}.
     *
     * @param chunk       buffer that contains the objects to serialize
     * @param validLength number of valid entries inside {@code chunk}
     * @param mode        the serialization mode
     * @return serialized bytes
     * @throws IOException if serialization fails
     */
    public static byte[] serializeChunk(Object[] chunk, int validLength, ObjectFileSerializationMode mode) throws IOException {
        Objects.requireNonNull(mode, "Serialization mode must be provided.");
        switch (mode) {
            case JSON:
                return serializeJson(chunk, validLength);
            case LEGACY_JAVA_SERIALIZATION:
                return serializeLegacy(chunk, validLength);
            default:
                throw new IllegalArgumentException("Unknown serialization mode: " + mode);
        }
    }

    /**
     * Deserialize a chunk of objects.
     *
     * @param payload     the serialized data
     * @param mode        the serialization mode
     * @param elementType the expected element type
     * @return list of deserialized objects (never {@code null})
     * @throws IOException            if deserialization fails
     * @throws ClassNotFoundException if a class cannot be resolved in legacy mode
     */
    public static List<Object> deserializeChunk(byte[] payload,
                                                ObjectFileSerializationMode mode,
                                                Class<?> elementType) throws IOException, ClassNotFoundException {
        Objects.requireNonNull(mode, "Serialization mode must be provided.");
        switch (mode) {
            case JSON:
                return deserializeJson(payload, elementType);
            case LEGACY_JAVA_SERIALIZATION:
                return deserializeLegacy(payload, elementType);
            default:
                throw new IllegalArgumentException("Unknown serialization mode: " + mode);
        }
    }

    private static byte[] serializeLegacy(Object[] chunk, int validLength) throws IOException {
        Object[] payload = Arrays.copyOf(chunk, validLength);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(payload);
            oos.flush();
            return bos.toByteArray();
        }
    }

    private static List<Object> deserializeLegacy(byte[] payload, Class<?> elementType) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(payload);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            ois.setObjectInputFilter(createObjectInputFilter(elementType));
            Object tmp = ois.readObject();
            if (tmp == null) {
                return Collections.emptyList();
            }
            if (tmp instanceof Collection) {
                return new ArrayList<>((Collection<?>) tmp);
            }
            if (tmp.getClass().isArray()) {
                return Arrays.asList((Object[]) tmp);
            }
            return new ArrayList<>(Collections.singletonList(tmp));
        }
    }

    /**
     * Creates an {@link ObjectInputFilter} allowlist for legacy Java deserialization.
     *
     * @param elementType the expected element type (optional, may be {@code null})
     * @return an {@link ObjectInputFilter} that permits only expected and safe classes
     */
    public static ObjectInputFilter createObjectInputFilter(Class<?> elementType) {
        ObjectInputFilter filter = filterInfo -> {
            Class<?> clazz = filterInfo.serialClass();
            if (clazz == null) {
                return ObjectInputFilter.Status.UNDECIDED;
            }
            if (isAllowedClass(clazz, elementType)) {
                return ObjectInputFilter.Status.ALLOWED;
            }
            return ObjectInputFilter.Status.REJECTED;
        };

        ObjectInputFilter globalFilter = ObjectInputFilter.Config.getSerialFilter();
        if (globalFilter != null) {
            return ObjectInputFilter.merge(globalFilter, filter);
        }
        return filter;
    }

    private static boolean isAllowedClass(Class<?> clazz, Class<?> elementType) {
        Class<?> current = clazz;
        while (current.isArray()) {
            current = current.getComponentType();
        }
        if (current.isPrimitive()) {
            return true;
        }

        // Standard base object and scalar types
        if (current == Object.class ||
                current == String.class ||
                current == Boolean.class ||
                current == Character.class ||
                current == Class.class ||
                Number.class.isAssignableFrom(current) ||
                Enum.class.isAssignableFrom(current) ||
                CharSequence.class.isAssignableFrom(current)) {
            return true;
        }

        // Standard math
        if (current.getName().startsWith("java.math.")) {
            return true;
        }

        // Standard date and time types
        if (current.getName().startsWith("java.time.") ||
                Date.class.isAssignableFrom(current)) {
            return true;
        }

        // Standard collections, maps, and common utilities
        if (Collection.class.isAssignableFrom(current) ||
                Map.class.isAssignableFrom(current) ||
                Map.Entry.class.isAssignableFrom(current) ||
                current.getName().startsWith("java.util.Collections$") ||
                current.getName().startsWith("java.util.Arrays$") ||
                current.getName().startsWith("java.util.ImmutableCollections$") ||
                current == UUID.class ||
                current == BitSet.class ||
                current == Locale.class ||
                current == Currency.class ||
                Optional.class.isAssignableFrom(current)) {
            return true;
        }

        // Wayang internal types
        if (current.getName().startsWith("org.apache.wayang.")) {
            return true;
        }

        // Scala tuples and basic collections (for Spark / Scala platform compatibility)
        if (current.getName().startsWith("scala.Tuple") ||
                current.getName().startsWith("scala.collection.")) {
            return true;
        }

        // Expected element type if specified
        if (elementType != null && !Object.class.equals(elementType)) {
            Class<?> targetComponent = elementType;
            while (targetComponent.isArray()) {
                targetComponent = targetComponent.getComponentType();
            }
            if (targetComponent.isAssignableFrom(current)) {
                return true;
            }
            if (current.getName().startsWith(targetComponent.getName() + "$")) {
                return true;
            }
            Package pkg = targetComponent.getPackage();
            if (pkg != null && current.getName().startsWith(pkg.getName() + ".")) {
                return true;
            }
        }

        // Configurable system property allowlist (comma/semicolon-separated patterns)
        String configuredAllowlist = System.getProperty("wayang.basic.serialization.allowlist",
                System.getProperty("wayang.serialization.allowlist"));
        if (configuredAllowlist != null && !configuredAllowlist.trim().isEmpty()) {
            String[] patterns = configuredAllowlist.split("[,;]");
            for (String pattern : patterns) {
                if (matchesPattern(pattern.trim(), current.getName())) {
                    return true;
                }
            }
        }

        return false;
    }

    private static boolean matchesPattern(String pattern, String className) {
        if (pattern.isEmpty()) {
            return false;
        }
        if (pattern.equals("*") || pattern.equals("**")) {
            return true;
        }
        if (pattern.endsWith(".**")) {
            String prefix = pattern.substring(0, pattern.length() - 3);
            return className.equals(prefix) || className.startsWith(prefix + ".");
        }
        if (pattern.endsWith(".*")) {
            String prefix = pattern.substring(0, pattern.length() - 2);
            if (className.startsWith(prefix + ".")) {
                String remainder = className.substring(prefix.length() + 1);
                return !remainder.contains(".");
            }
            return false;
        }
        return pattern.equals(className);
    }

    private static byte[] serializeJson(Object[] chunk, int validLength) throws IOException {
        Object[] payload = Arrays.copyOf(chunk, validLength);
        return OBJECT_MAPPER.writeValueAsBytes(payload);
    }

    private static List<Object> deserializeJson(byte[] payload, Class<?> elementType) throws IOException {
        Objects.requireNonNull(elementType, "Element type must be provided for JSON deserialization.");
        CollectionType type = OBJECT_MAPPER.getTypeFactory()
                .constructCollectionType(List.class, elementType);
        List<?> list = OBJECT_MAPPER.readValue(payload, type);
        if (list == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(list);
    }
}
