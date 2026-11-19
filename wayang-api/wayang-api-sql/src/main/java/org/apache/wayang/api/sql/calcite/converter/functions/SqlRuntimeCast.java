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

package org.apache.wayang.api.sql.calcite.converter.functions;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;

import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;

/**
 * Runtime SQL {@code CAST} for Wayang Java filter evaluation, delegating to
 * {@link SqlFunctions} where possible.
 */
public final class SqlRuntimeCast {

    private SqlRuntimeCast() {}

    /**
     * @param input  evaluated operand (SQL NULL is {@code null})
     * @param target destination SQL type name of the cast (from the RexCall result type)
     * @return value suitable for comparisons and filter logic
     */
    public static Object castValue(final Object input, final SqlTypeName target) {
        if (input == null) {
            return null;
        }
        final Object v = unwrapForCast(input);
        switch (target) {
            case BOOLEAN:
                return SqlFunctions.toBoolean(v);
            case TINYINT:
                return SqlFunctions.toByte(v);
            case SMALLINT:
                return SqlFunctions.toShort(v);
            case INTEGER:
                return SqlFunctions.toInt(v);
            case BIGINT:
                return SqlFunctions.toLong(v);
            case DECIMAL:
                return SqlFunctions.toBigDecimal(v);
            case FLOAT:
            case REAL:
                return castToFloat(v);
            case DOUBLE:
                return castToDouble(v);
            case CHAR:
            case VARCHAR:
                return castToString(v);
            default:
                throw new UnsupportedOperationException(
                        "CAST to " + target + " is not supported in Java filter evaluation yet.");
        }
    }

    private static Object unwrapForCast(final Object o) {
        if (o instanceof NlsString) {
            return ((NlsString) o).getValue();
        }
        if (o instanceof Character) {
            return o.toString();
        }
        return o;
    }

    private static float castToFloat(final Object v) {
        if (v instanceof DateString) {
            return (float) ((DateString) v).getMillisSinceEpoch();
        }
        if (v instanceof Date) {
            return (float) ((Date) v).getTime();
        }
        if (v instanceof Calendar) {
            return (float) ((Calendar) v).getTimeInMillis();
        }
        return SqlFunctions.toFloat(v);
    }

    private static double castToDouble(final Object v) {
        if (v instanceof DateString) {
            return (double) ((DateString) v).getMillisSinceEpoch();
        }
        if (v instanceof Date) {
            return (double) ((Date) v).getTime();
        }
        if (v instanceof Calendar) {
            return (double) ((Calendar) v).getTimeInMillis();
        }
        return SqlFunctions.toDouble(v);
    }

    private static String castToString(final Object v) {
        if (v instanceof String) {
            return (String) v;
        }
        if (v instanceof NlsString) {
            return ((NlsString) v).getValue();
        }
        if (v instanceof Boolean) {
            return SqlFunctions.toString((Boolean) v);
        }
        if (v instanceof Float) {
            return SqlFunctions.toString((Float) v);
        }
        if (v instanceof Double) {
            return SqlFunctions.toString((Double) v);
        }
        if (v instanceof BigDecimal) {
            return SqlFunctions.toString((BigDecimal) v);
        }
        if (v instanceof Number) {
            return ((Number) v).toString();
        }
        if (v instanceof DateString) {
            return v.toString();
        }
        if (v instanceof Character) {
            return v.toString();
        }
        if (v instanceof Date) {
            return v.toString();
        }
        if (v instanceof Calendar) {
            return ((Calendar) v).getTime().toString();
        }
        return String.valueOf(v);
    }
}
