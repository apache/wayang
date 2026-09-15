/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.jdbc.protocol.io;

import org.apache.wayang.jdbc.protocol.MessageEnvelope;
import org.apache.wayang.jdbc.protocol.MessageType;
import org.apache.wayang.jdbc.protocol.ProtocolConstants;
import org.apache.wayang.jdbc.protocol.ProtocolException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Encodes and decodes length-prefixed JSON protocol messages.
 */
public class ProtocolMessageCodec {

    private final ObjectMapper objectMapper;

    private final int maxFrameLength;

    public ProtocolMessageCodec() {
        this(createDefaultObjectMapper(), ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH);
    }

    public ProtocolMessageCodec(final int maxFrameLength) {
        this(createDefaultObjectMapper(), maxFrameLength);
    }

    public ProtocolMessageCodec(final ObjectMapper objectMapper, final int maxFrameLength) {
        if (objectMapper == null) {
            throw new IllegalArgumentException("Object mapper must not be null.");
        }
        if (maxFrameLength <= 0) {
            throw new IllegalArgumentException("Maximum frame length must be positive.");
        }
        this.objectMapper = objectMapper;
        this.maxFrameLength = maxFrameLength;
    }

    private static ObjectMapper createDefaultObjectMapper() {
        return new ObjectMapper()
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    }

    public MessageEnvelope toEnvelope(
            final String requestId,
            final MessageType type,
            final Object payload
    ) {
        final JsonNode payloadNode = this.objectMapper.valueToTree(payload);
        return new MessageEnvelope(requestId, type, payloadNode);
    }

    public <T> T payloadAs(final MessageEnvelope envelope, final Class<T> payloadClass) throws ProtocolException {
        try {
            return this.objectMapper.treeToValue(envelope.getPayload(), payloadClass);
        } catch (JsonProcessingException e) {
            throw new ProtocolException("Could not decode payload for message type " + envelope.getType(), e);
        }
    }

    public void write(final OutputStream outputStream, final MessageEnvelope message) throws IOException {
        this.validateEnvelope(message);

        final byte[] payload = this.objectMapper.writeValueAsBytes(message);
        if (payload.length > this.maxFrameLength) {
            throw new ProtocolException("Protocol frame exceeds maximum length: " + payload.length);
        }

        final DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        dataOutputStream.writeInt(payload.length);
        dataOutputStream.write(payload);
        dataOutputStream.flush();
    }

    public MessageEnvelope read(final InputStream inputStream) throws IOException {
        final DataInputStream dataInputStream = new DataInputStream(inputStream);
        final int frameLength;
        try {
            frameLength = dataInputStream.readInt();
        } catch (EOFException e) {
            return null;
        }

        this.validateFrameLength(frameLength);

        final byte[] payload = new byte[frameLength];
        dataInputStream.readFully(payload);

        final MessageEnvelope envelope = this.objectMapper.readValue(payload, MessageEnvelope.class);
        this.validateEnvelope(envelope);
        return envelope;
    }

    private void validateFrameLength(final int frameLength) throws ProtocolException {
        if (frameLength <= 0) {
            throw new ProtocolException("Protocol frame length must be positive: " + frameLength);
        }
        if (frameLength > this.maxFrameLength) {
            throw new ProtocolException("Protocol frame exceeds maximum length: " + frameLength);
        }
    }

    private void validateEnvelope(final MessageEnvelope envelope) throws ProtocolException {
        if (envelope == null) {
            throw new ProtocolException("Protocol message must not be null.");
        }
        if (envelope.getVersion() != ProtocolConstants.CURRENT_VERSION) {
            throw new ProtocolException("Unsupported protocol version: " + envelope.getVersion());
        }
        if (envelope.getRequestId() == null || envelope.getRequestId().isBlank()) {
            throw new ProtocolException("Protocol message is missing a request id.");
        }
        if (envelope.getType() == null) {
            throw new ProtocolException("Protocol message is missing a message type.");
        }
        if (envelope.getPayload() == null || envelope.getPayload().isNull()) {
            throw new ProtocolException("Protocol message is missing a payload.");
        }
    }
}
