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

package org.apache.wayang.jdbc.protocol;

import com.fasterxml.jackson.databind.JsonNode;


public class MessageEnvelope {

    private int version = ProtocolConstants.CURRENT_VERSION;

    private String requestId;

    private MessageType type;

    private JsonNode payload;

    public MessageEnvelope() {
    }

    public MessageEnvelope(
            final String requestId,
            final MessageType type,
            final JsonNode payload
    ) {
        this(ProtocolConstants.CURRENT_VERSION, requestId, type, payload);
    }

    public MessageEnvelope(
            final int version,
            final String requestId,
            final MessageType type,
            final JsonNode payload
    ) {
        this.version = version;
        this.requestId = requestId;
        this.type = type;
        this.payload = payload;
    }

    public int getVersion() {
        return this.version;
    }

    public void setVersion(final int version) {
        this.version = version;
    }

    public String getRequestId() {
        return this.requestId;
    }

    public void setRequestId(final String requestId) {
        this.requestId = requestId;
    }

    public MessageType getType() {
        return this.type;
    }

    public void setType(final MessageType type) {
        this.type = type;
    }

    public JsonNode getPayload() {
        return this.payload;
    }

    public void setPayload(final JsonNode payload) {
        this.payload = payload;
    }
}
