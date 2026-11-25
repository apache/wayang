#
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import base64
import io
import json
import struct
from unittest.mock import Mock

import cloudpickle
import pytest
import requests

from pywy.dataquanta import WayangContext
from pywy.execution.util import JsonRecord
from pywy.execution.worker import dump_stream, write_with_length
from pywy.platforms.java import JavaPlugin


@pytest.fixture
def post(monkeypatch):
    response = requests.Response()
    response.status_code = 200
    response._content = b'{"results": []}'
    post = Mock(return_value=response)
    monkeypatch.setattr("pywy.core.core.requests.post", post)
    return post


@pytest.fixture
def context():
    return WayangContext().register({JavaPlugin})


@pytest.mark.parametrize("values", [
    [],
    [-(2 ** 63), 0, 2 ** 63 - 1, 1.25, -0.0],
    [True, False, "123", "true", "null", "", 'quote " \\', "雪 😀", "line\n\t\0"],
    [[1, [False, "雪"]], []],
])
def test_collect_response(context, post, values):
    post.return_value._content = json.dumps({"results": values}).encode("utf-8")
    result = context.load_collection([]).collect()
    # JSON equality also distinguishes booleans, integers, floats and negative zero.
    assert json.dumps(result) == json.dumps(values)


@pytest.mark.parametrize("body", [
    b"Success", b"Buffer(1, 2)", b"{", b"",
    b"[]", b"null", b"{}", b'{"results": null}', b'{"results": {}}',
    b'{"results": "[]"}',
])
def test_rejects_malformed_responses(context, post, body):
    post.return_value._content = body
    with pytest.raises(ValueError, match="Wayang collect expected"):
        context.load_collection([]).collect()


@pytest.mark.parametrize("body", [
    b'{"results": [null]}', b'{"results": [{}]}',
    b'{"results": [9223372036854775808]}', b'{"results": [-9223372036854775809]}',
    b'{"results": [NaN]}', b'{"results": [1e400]}',
])
def test_rejects_unsupported_response_values(context, post, body):
    post.return_value._content = body
    with pytest.raises(ValueError, match="Unsupported Wayang collect response: results"):
        context.load_collection([]).collect()


@pytest.mark.parametrize("status", [400, 500])
def test_reports_backend_error(context, post, status):
    post.return_value.status_code = status
    post.return_value._content = b"Python collect failed: ZeroDivisionError: division by zero"
    with pytest.raises(requests.HTTPError, match="ZeroDivisionError") as failure:
        context.load_collection([1]).collect()
    assert failure.value.response is post.return_value
    assert f"HTTP {status}" in str(failure.value)


def test_propagates_connection_failure(context, post):
    post.side_effect = requests.ConnectionError("backend unavailable")
    with pytest.raises(requests.ConnectionError, match="backend unavailable"):
        context.load_collection([]).collect()


def test_serializes_terminal_pipeline(context, post):
    (context.load_collection([1, 2, 3])
     .map(lambda value: int(value) * 2, str, int)
     .filter(lambda value: value > 2, int)
     .collect())
    plan = post.call_args.kwargs["json"]
    source, terminal = plan["operators"]
    assert plan["collect"] is True
    assert plan["context"]["platforms"] == ["java"]
    assert source["output"] == [terminal["id"]]
    assert terminal["input"] == [source["id"]]
    assert terminal["output"] == []
    assert terminal["operatorName"] == "mapPartitions"
    assert terminal["data"]["jsonOutput"] is True
    assert "outputType" not in terminal["data"]
    udf = cloudpickle.loads(base64.b64decode(terminal["data"]["udf"]))
    records = list(udf(iter(["1", "2", "3"])))
    assert all(type(record) is JsonRecord for record in records)
    assert [json.loads(record) for record in records] == [4, 6]


def test_collect_does_not_change_graph(context, post):
    source = context.load_collection([1])
    mapped = source.map(lambda value: int(value), str, int)
    source.collect()
    source_plan = post.call_args.kwargs["json"]
    assert len(source_plan["operators"]) == 1
    assert source_plan["operators"][0]["output"] == []
    assert source.operator.outputOperator == [mapped.operator]
    mapped.collect()
    mapped.collect()
    assert post.call_count == 3
    assert source.operator.outputOperator == [mapped.operator]
    assert mapped.operator.outputOperator == [None]


def test_text_sink_keeps_legacy_request(context, post):
    assert context.load_collection([1]).store_textfile("file:///tmp/output.txt") is None
    plan = post.call_args.kwargs["json"]
    assert "collect" not in plan
    assert plan["operators"][-1]["operatorName"] == "textFileOutput"


def test_collect_earlier_branch(context, post):
    source = context.load_collection([1])
    first = source.map(lambda value: int(value) * 2, str, int)
    second = source.map(lambda value: int(value) * 3, str, int)
    first.collect()
    source_plan, terminal = post.call_args.kwargs["json"]["operators"]
    assert source_plan["output"] == [terminal["id"]]
    assert terminal["input"] == [source_plan["id"]]
    assert source.operator.outputOperator == [second.operator]


@pytest.mark.parametrize("payload", [b'""', b"false", b"123", b'[1,true]', '"雪"'.encode()])
def test_worker_frames_each_json_record_once(payload):
    stream = io.BytesIO()
    dump_stream(iter([JsonRecord(payload)]), stream)
    assert stream.getvalue() == struct.pack("!i", len(payload)) + payload + struct.pack("!i", -1)


def test_worker_preserves_legacy_byte_values():
    stream = io.BytesIO()
    write_with_length(b"ab", stream)
    assert stream.getvalue() == b"\x00\x00\x00\x05b'ab'"
