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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.junit.jupiter.api.Test;

class SqlRuntimeCastTest {

    @Test
    void castNullYieldsNull() {
        assertNull(SqlRuntimeCast.castValue(null, SqlTypeName.INTEGER));
    }

    @Test
    void castIntegerToVarchar() {
        assertEquals("1", SqlRuntimeCast.castValue(1, SqlTypeName.VARCHAR));
    }

    @Test
    void castStringToInteger() {
        assertEquals(42, SqlRuntimeCast.castValue("42", SqlTypeName.INTEGER));
    }

    @Test
    void castStringToDouble() {
        assertEquals(1.5d, (Double) SqlRuntimeCast.castValue("1.5", SqlTypeName.DOUBLE), 1e-9);
    }

    @Test
    void castNlsStringToInteger() {
        final NlsString nls = new NlsString("7", "UTF-8", null);
        assertEquals(7, SqlRuntimeCast.castValue(nls, SqlTypeName.INTEGER));
    }

    @Test
    void castStringToBoolean() {
        assertTrue(SqlRuntimeCast.castValue("TRUE", SqlTypeName.BOOLEAN) instanceof Boolean);
        assertEquals(true, SqlRuntimeCast.castValue("TRUE", SqlTypeName.BOOLEAN));
    }

    @Test
    void castInvalidBooleanThrows() {
        assertThrows(RuntimeException.class, () -> SqlRuntimeCast.castValue("maybe", SqlTypeName.BOOLEAN));
    }

    @Test
    void castBigDecimalToVarcharUsesSqlFormat() {
        final String s = SqlRuntimeCast.castValue(BigDecimal.valueOf(1, 1), SqlTypeName.VARCHAR).toString();
        assertTrue(s.contains("1"));
    }

    @Test
    void castToDateUnsupported() {
        assertThrows(UnsupportedOperationException.class,
                () -> SqlRuntimeCast.castValue("2020-01-01", SqlTypeName.DATE));
    }
}
