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
import java.io.InvalidClassException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Currency;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import org.apache.wayang.core.api.Configuration;

/**
 * Utility methods that convert between Java objects and the on-disk representation used by {@link ObjectFileSink}.
 */
public final class ObjectFileSerialization {

    public static final String ALLOWLIST_PROPERTY = "wayang.basic.serialization.allowlist";
    public static final String LEGACY_ALLOWLIST_PROPERTY = "wayang.serialization.allowlist";
    public static final String FLINK_ALLOWLIST_PROPERTY = "wayang.flink.serialization.allowlist";

    public static final String EXPECTED_TYPE_PROPERTY = "wayang.basic.serialization.expected-type";
    public static final String LEGACY_EXPECTED_TYPE_PROPERTY = "wayang.serialization.expected-type";
    public static final String FLINK_EXPECTED_TYPE_PROPERTY = "wayang.flink.value.expected-type";

    public static final String MAX_DEPTH_PROPERTY = "wayang.basic.serialization.max-depth";
    public static final String LEGACY_MAX_DEPTH_PROPERTY = "wayang.serialization.max-depth";
    public static final String MAX_ARRAY_LENGTH_PROPERTY = "wayang.basic.serialization.max-array-length";
    public static final String LEGACY_MAX_ARRAY_LENGTH_PROPERTY = "wayang.serialization.max-array-length";
    public static final String MAX_REFERENCES_PROPERTY = "wayang.basic.serialization.max-references";
    public static final String LEGACY_MAX_REFERENCES_PROPERTY = "wayang.serialization.max-references";
    public static final String MAX_BYTES_PROPERTY = "wayang.basic.serialization.max-bytes";
    public static final String LEGACY_MAX_BYTES_PROPERTY = "wayang.serialization.max-bytes";

    static final long DEFAULT_MAX_DEPTH = 100L;
    static final long DEFAULT_MAX_ARRAY_LENGTH = 1_048_576L;
    static final long DEFAULT_MAX_REFERENCES = 100_000L;
    static final long DEFAULT_MAX_BYTES = 67_108_864L;

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
        long maxBytes = getMaxBytes();
        if (payload != null && payload.length > maxBytes) {
            throw new InvalidClassException("Serialized payload length " + payload.length + " exceeds max bytes limit " + maxBytes);
        }
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
     * Creates an {@link ObjectInputFilter} enforcing resource limits and class allowlists for legacy Java deserialization.
     *
     * @param elementType the expected element type (optional, may be {@code null})
     * @return an {@link ObjectInputFilter} that permits only expected, safe classes within configured resource bounds
     */
    public static ObjectInputFilter createObjectInputFilter(Class<?> elementType) {
        final long maxDepth = getMaxDepth();
        final long maxArrayLength = getMaxArrayLength();
        final long maxReferences = getMaxReferences();
        final long maxBytes = getMaxBytes();

        ObjectInputFilter filter = filterInfo -> {
            // Check stream/graph resource bounds before serialClass() since limit callbacks may pass serialClass == null.
            if (filterInfo.depth() > maxDepth ||
                    filterInfo.arrayLength() > maxArrayLength ||
                    filterInfo.references() > maxReferences ||
                    filterInfo.streamBytes() > maxBytes) {
                return ObjectInputFilter.Status.REJECTED;
            }

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
                current == Void.class ||
                current == java.lang.Record.class ||
                isStandardNumber(current) ||
                Enum.class.isAssignableFrom(current) ||
                isStandardCharSequence(current)) {
            return true;
        }

        // Standard math
        if (current.getName().startsWith("java.math.")) {
            return true;
        }

        // Standard date and time types
        if (current.getName().startsWith("java.time.") ||
                (Date.class.isAssignableFrom(current) &&
                        (current.getName().startsWith("java.util.") || current.getName().startsWith("java.sql.")))) {
            return true;
        }

        // Standard collections, maps, and common utilities
        if (((Collection.class.isAssignableFrom(current) ||
                Map.class.isAssignableFrom(current) ||
                Map.Entry.class.isAssignableFrom(current)) &&
                current.getName().startsWith("java.util.")) ||
                current.getName().equals("java.util.CollSer") ||
                current.getName().startsWith("java.util.Collections$") ||
                current.getName().startsWith("java.util.Arrays$") ||
                current.getName().startsWith("java.util.ImmutableCollections$") ||
                current == UUID.class ||
                current == BitSet.class ||
                current == Locale.class ||
                current == Currency.class ||
                current == Optional.class) {
            return true;
        }

        // Wayang internal data quantum types
        if (current.getName().equals("org.apache.wayang.basic.data.Record") ||
                current.getName().equals("org.apache.wayang.basic.data.Tuple2") ||
                current.getName().equals("org.apache.wayang.basic.data.Tuple5") ||
                current.getName().equals("org.apache.wayang.core.util.Tuple") ||
                current.getName().equals("org.apache.wayang.basic.model.op.Op") ||
                current.getName().equals("org.apache.wayang.flink.compiler.criterion.WayangValue") ||
                current.getName().equals("org.apache.wayang.flink.compiler.criterion.WayangListValue")) {
            return true;
        }

        // Scala tuples and basic collections (for Spark / Scala platform compatibility)
        if (current.getName().startsWith("scala.Tuple") ||
                current.getName().startsWith("scala.collection.")) {
            return true;
        }

        // Expected element type if specified
        if (elementType != null && !isBroadType(elementType)) {
            Class<?> targetComponent = elementType;
            while (targetComponent.isArray()) {
                targetComponent = targetComponent.getComponentType();
            }
            if (!isBroadType(targetComponent) &&
                    !Number.class.isAssignableFrom(targetComponent) &&
                    !Collection.class.isAssignableFrom(targetComponent) &&
                    !Map.class.isAssignableFrom(targetComponent)) {
                if (current.equals(targetComponent) ||
                        (current.getName().startsWith(targetComponent.getName() + "$") &&
                                !Number.class.isAssignableFrom(current) &&
                                !Collection.class.isAssignableFrom(current) &&
                                !Map.class.isAssignableFrom(current))) {
                    return true;
                }
            }
        }

        // Trusted configuration allowlist (comma/semicolon-separated patterns via system property or Wayang Configuration)
        String basicAllowlist = resolveConfigProperty(ALLOWLIST_PROPERTY, LEGACY_ALLOWLIST_PROPERTY);
        if (matchesAllowlistPatterns(basicAllowlist, current.getName())) {
            return true;
        }
        String flinkAllowlist = resolveConfigProperty(FLINK_ALLOWLIST_PROPERTY);
        return matchesAllowlistPatterns(flinkAllowlist, current.getName());
    }

    /**
     * Resolves the first non-blank value among {@code keys}, checking JVM system properties before Wayang's
     * default {@link Configuration}.
     *
     * @param keys ordered property keys from highest to lowest precedence
     * @return trimmed property value, or {@code null} if unset or blank
     */
    public static String resolveConfigProperty(String... keys) {
        if (keys == null) {
            return null;
        }
        for (String key : keys) {
            if (key == null) {
                continue;
            }
            String sysValue = System.getProperty(key);
            if (sysValue != null && !sysValue.trim().isEmpty()) {
                return sysValue.trim();
            }
        }
        Configuration defaultConfig = Configuration.getDefaultConfiguration();
        if (defaultConfig != null) {
            for (String key : keys) {
                if (key == null) {
                    continue;
                }
                Optional<String> configValue = defaultConfig.getOptionalStringProperty(key);
                if (configValue.isPresent() && !configValue.get().trim().isEmpty()) {
                    return configValue.get().trim();
                }
            }
        }
        return null;
    }

    /**
     * Returns the configured maximum object graph nesting depth for legacy Java deserialization.
     *
     * @return positive depth limit
     */
    public static long getMaxDepth() {
        return getPositiveLongProperty(DEFAULT_MAX_DEPTH, MAX_DEPTH_PROPERTY, LEGACY_MAX_DEPTH_PROPERTY);
    }

    /**
     * Returns the configured maximum array length for legacy Java deserialization.
     *
     * @return positive array-length limit
     */
    public static long getMaxArrayLength() {
        return getPositiveLongProperty(DEFAULT_MAX_ARRAY_LENGTH, MAX_ARRAY_LENGTH_PROPERTY, LEGACY_MAX_ARRAY_LENGTH_PROPERTY);
    }

    /**
     * Returns the configured maximum object reference count for legacy Java deserialization.
     *
     * @return positive reference-count limit
     */
    public static long getMaxReferences() {
        return getPositiveLongProperty(DEFAULT_MAX_REFERENCES, MAX_REFERENCES_PROPERTY, LEGACY_MAX_REFERENCES_PROPERTY);
    }

    /**
     * Returns the configured maximum stream byte count for legacy Java deserialization.
     *
     * @return positive stream-bytes limit
     */
    public static long getMaxBytes() {
        return getPositiveLongProperty(DEFAULT_MAX_BYTES, MAX_BYTES_PROPERTY, LEGACY_MAX_BYTES_PROPERTY);
    }

    private static long getPositiveLongProperty(long defaultValue, String... keys) {
        String raw = resolveConfigProperty(keys);
        if (raw == null) {
            return defaultValue;
        }
        try {
            long parsed = Long.parseLong(raw);
            if (parsed <= 0) {
                throw new IllegalArgumentException("Serialization limit property must be positive: " + raw);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric serialization limit property: " + raw, e);
        }
    }

    private static boolean matchesAllowlistPatterns(String configuredAllowlist, String className) {
        if (configuredAllowlist == null || configuredAllowlist.trim().isEmpty()) {
            return false;
        }
        String[] patterns = configuredAllowlist.split("[,;]");
        for (String pattern : patterns) {
            if (matchesPattern(pattern.trim(), className)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isStandardNumber(Class<?> clazz) {
        return clazz == Number.class ||
                clazz == Byte.class ||
                clazz == Short.class ||
                clazz == Integer.class ||
                clazz == Long.class ||
                clazz == Float.class ||
                clazz == Double.class;
    }

    private static boolean isStandardCharSequence(Class<?> clazz) {
        return clazz == String.class ||
                clazz == StringBuilder.class ||
                clazz == StringBuffer.class ||
                clazz == CharSequence.class;
    }

    private static boolean isBroadType(Class<?> clazz) {
        return clazz == Object.class ||
                clazz == Serializable.class ||
                clazz == Comparable.class ||
                clazz == Cloneable.class ||
                clazz == AutoCloseable.class ||
                clazz == Number.class ||
                clazz == CharSequence.class ||
                clazz == Collection.class ||
                clazz == List.class ||
                clazz == Set.class ||
                clazz == Queue.class ||
                clazz == Map.class ||
                clazz == Map.Entry.class ||
                clazz == Iterable.class ||
                clazz == Iterator.class ||
                clazz == Throwable.class ||
                clazz == Exception.class ||
                clazz == Error.class ||
                clazz == Enum.class ||
                clazz == java.lang.Record.class;
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
