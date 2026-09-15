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

package org.apache.wayang.jdbc.protocol.message;

/**
 * Requests cancellation of a running statement.
 */
public class CancelQueryRequest {

    private String connectionId;

    private String statementId;

    private String cursorId;

    public CancelQueryRequest() {
    }

    public CancelQueryRequest(
            final String connectionId,
            final String statementId,
            final String cursorId
    ) {
        this.connectionId = connectionId;
        this.statementId = statementId;
        this.cursorId = cursorId;
    }

    public String getConnectionId() {
        return this.connectionId;
    }

    public void setConnectionId(final String connectionId) {
        this.connectionId = connectionId;
    }

    public String getStatementId() {
        return this.statementId;
    }

    public void setStatementId(final String statementId) {
        this.statementId = statementId;
    }

    public String getCursorId() {
        return this.cursorId;
    }

    public void setCursorId(final String cursorId) {
        this.cursorId = cursorId;
    }
}
