#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from io import BytesIO
import struct

import pytest

from pywy.execution.util import SpecialLengths
from pywy.execution.worker import UTF8Deserializer, dump_stream


@pytest.mark.parametrize("values, expected", [
    (["hello"], ["hello"]),
    (["first", "second"], ["first", "second"]),
    (["same", "same"], ["same", "same"]),
    ([""], [""]),
    (["雪 😀", "line\nfeed\ttab"], ["雪 😀", "line\nfeed\ttab"]),
    ([True, False], ["1", "0"]),
    ([0, -2, 3.5, -0.0], ["0", "-2", "3.5", "-0.0"]),
    (["hello", True, 7, False, "bye", 2.5], ["hello", "1", "7", "0", "bye", "2.5"]),
    ([], []),
    (True, ["1"]),
    (False, ["0"]),
])
def test_dump_stream(values, expected):
    stream = BytesIO()
    dump_stream(values if type(values) is bool else iter(values), stream)

    stream.seek(0)
    assert list(UTF8Deserializer().load_stream(stream)) == expected

    # Verify the actual frames, including exactly one end marker.
    payloads = [value.encode("utf-8") for value in expected]
    expected_bytes = b"".join(struct.pack("!i", len(value)) + value for value in payloads)
    expected_bytes += struct.pack("!i", SpecialLengths.END_OF_DATA_SECTION)
    assert stream.getvalue() == expected_bytes
