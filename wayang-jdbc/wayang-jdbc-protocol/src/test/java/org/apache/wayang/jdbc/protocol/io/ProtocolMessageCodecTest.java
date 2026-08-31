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
import org.apache.wayang.jdbc.protocol.ProtocolException;
import org.apache.wayang.jdbc.protocol.message.ColumnInfo;
import org.apache.wayang.jdbc.protocol.message.PingRequest;
import org.apache.wayang.jdbc.protocol.message.QueryResultResponse;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtocolMessageCodecTest {

    @Test
    void roundTripsPingRequest() throws Exception {
        final ProtocolMessageCodec codec = new ProtocolMessageCodec();
        final MessageEnvelope request = codec.toEnvelope(
                "request-1",
                MessageType.PING,
                new PingRequest("connection-1")
        );

        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        codec.write(output, request);

        final MessageEnvelope decoded = codec.read(new ByteArrayInputStream(output.toByteArray()));
        final PingRequest payload = codec.payloadAs(decoded, PingRequest.class);

        assertEquals("request-1", decoded.getRequestId());
        assertEquals(MessageType.PING, decoded.getType());
        assertEquals("connection-1", payload.getConnectionId());
    }

    @Test
    void preservesDecimalRows() throws Exception {
        final ProtocolMessageCodec codec = new ProtocolMessageCodec();
        final QueryResultResponse response = new QueryResultResponse(
                "connection-1",
                "statement-1",
                List.of(new ColumnInfo(
                        "AMOUNT",
                        "AMOUNT",
                        null,
                        null,
                        "DECIMAL",
                        Types.DECIMAL,
                        ResultSetMetaData.columnNullable,
                        10,
                        2
                )),
                List.of(Collections.singletonList(new BigDecimal("12.34"))),
                false,
                null
        );

        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        codec.write(output, codec.toEnvelope("request-2", MessageType.QUERY_RESULT, response));

        final MessageEnvelope decoded = codec.read(new ByteArrayInputStream(output.toByteArray()));
        final QueryResultResponse payload = codec.payloadAs(decoded, QueryResultResponse.class);
        final Object value = payload.getRows().get(0).get(0);

        assertInstanceOf(BigDecimal.class, value);
        assertEquals(new BigDecimal("12.34"), value);
    }

    @Test
    void rejectsFramesAboveConfiguredLimit() {
        final ProtocolMessageCodec codec = new ProtocolMessageCodec(8);
        final MessageEnvelope request = codec.toEnvelope(
                "request-3",
                MessageType.PING,
                new PingRequest("connection-1")
        );

        assertThrows(ProtocolException.class, () -> codec.write(new ByteArrayOutputStream(), request));
    }
}
