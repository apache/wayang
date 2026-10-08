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
 * Carries an error that should be surfaced by the JDBC driver as a {@link java.sql.SQLException}.
 */
public class ErrorResponse {

    private ErrorCode errorCode;

    private String sqlState;

    private int vendorCode;

    private String message;

    private String detail;

    private String exceptionClass;

    public ErrorResponse() {
    }

    public ErrorResponse(
            final String sqlState,
            final int vendorCode,
            final String message,
            final String detail
    ) {
        this(null, sqlState, vendorCode, message, detail, null);
    }

    public ErrorResponse(
            final ErrorCode errorCode,
            final String sqlState,
            final int vendorCode,
            final String message,
            final String detail,
            final String exceptionClass
    ) {
        this.errorCode = errorCode;
        this.sqlState = sqlState;
        this.vendorCode = vendorCode;
        this.message = message;
        this.detail = detail;
        this.exceptionClass = exceptionClass;
    }

    public ErrorCode getErrorCode() {
        return this.errorCode;
    }

    public void setErrorCode(final ErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    public String getSqlState() {
        return this.sqlState;
    }

    public void setSqlState(final String sqlState) {
        this.sqlState = sqlState;
    }

    public int getVendorCode() {
        return this.vendorCode;
    }

    public void setVendorCode(final int vendorCode) {
        this.vendorCode = vendorCode;
    }

    public String getMessage() {
        return this.message;
    }

    public void setMessage(final String message) {
        this.message = message;
    }

    public String getDetail() {
        return this.detail;
    }

    public void setDetail(final String detail) {
        this.detail = detail;
    }

    public String getExceptionClass() {
        return this.exceptionClass;
    }

    public void setExceptionClass(final String exceptionClass) {
        this.exceptionClass = exceptionClass;
    }
}
