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

"""Collect regression tests against PYWAYANG_INTEGRATION_URL, never a mocked backend."""

import json
import math
import os

import pytest
import requests

from pywy.configuration import Configuration
from pywy.dataquanta import WayangContext
from pywy.platforms.java import JavaPlugin
from pywy.platforms.spark import SparkPlugin


@pytest.fixture
def api(monkeypatch):
    url = os.environ.get("PYWAYANG_INTEGRATION_URL")
    if not url:
        pytest.skip("Set PYWAYANG_INTEGRATION_URL to run against a local Wayang backend")
    post = requests.post

    def submit(*args, **kwargs):
        kwargs.setdefault("timeout", 120)
        return post(*args, **kwargs)

    monkeypatch.setattr(requests, "post", submit)
    return url


@pytest.fixture
def context(api):
    configuration = Configuration()
    configuration.set_property("wayang.api.python.url", api)
    return WayangContext(configuration).register({JavaPlugin})


@pytest.mark.parametrize("values", [
    [],
    [-(2 ** 63), -1, 0, 2 ** 63 - 1],
    [1.5, -0.25, -0.0, 1.23456789012345],
    [True, False, "123", "true", "null", ""],
    ['quotes " and \\', "line\nfeed\ttab\0", "雪 😀", '${System.exit(1)}'],
    [[1, [False, "雪"]], [], [2.5, True]],
])
def test_native_collection_round_trip(context, values):
    result = context.load_collection(values).collect()
    assert json.dumps(result) == json.dumps(values)


@pytest.mark.parametrize("function, expected", [
    (lambda value: int(value) * 2, [2, 4, 6]),
    (lambda value: float(value) / 2, [0.5, 1.0, 1.5]),
    (lambda value: int(value) > 1, [False, True, True]),
    (lambda value: "", ["", "", ""]),
    (lambda value: "雪\n\t\x00" + value, ["雪\n\t\x001", "雪\n\t\x002", "雪\n\t\x003"]),
    (lambda value: [int(value), (True, ["雪", 2.5])],
     [[1, [True, ["雪", 2.5]]], [2, [True, ["雪", 2.5]]], [3, [True, ["雪", 2.5]]]]),
], ids=["integers", "floats", "booleans", "empty-strings", "unicode", "nested"])
def test_transformed_values(context, function, expected):
    result = context.load_collection([1, 2, 3]).map(function, str, str).collect()
    assert json.dumps(result) == json.dumps(expected)


def test_required_example_and_runtime_type(context):
    result = (context.load_collection([1, 2, 3])
              .map(lambda value: int(value) * 2, str, int).collect())
    assert result == [2, 4, 6]
    assert all(type(value) is int for value in result)
    assert context.load_collection([1]).map(lambda value: "123", str, int).collect() == ["123"]


def test_filter_and_map(context):
    source = context.load_collection([1, 2, 3])
    assert source.filter(lambda value: int(value) > 1, str).collect() == ["2", "3"]
    assert (source.map(lambda value: int(value) * 2, str, int)
            .filter(lambda value: value > 2, int).collect()) == [4, 6]
    assert (source.filter(lambda value: int(value) > 1, str)
            .map(lambda value: int(value) * 2, str, int).collect()) == [4, 6]


def test_empty_transformations(context):
    assert context.load_collection([]).map(lambda value: int(value), str, int).collect() == []
    assert context.load_collection([1, 2]).filter(lambda value: False, str).collect() == []
    assert (context.load_collection([1, 2]).map(lambda value: int(value), str, int)
            .filter(lambda value: value > 10, int).collect()) == []


def test_repeat_collect_and_text_sink(context, tmp_path):
    source = context.load_collection([1, 2, 3])
    mapped = source.map(lambda value: int(value) * 2, str, int)
    assert source.collect() == [1, 2, 3]
    assert mapped.collect() == [2, 4, 6]
    assert mapped.collect() == [2, 4, 6]
    output = tmp_path / "values.txt"
    assert source.store_textfile(output.as_uri()) is None
    assert output.read_text() == "1\n2\n3\n"
    assert source.collect() == [1, 2, 3]
    assert mapped.collect() == [2, 4, 6]


def test_collect_earlier_branch(context):
    source = context.load_collection([1, 2, 3])
    first = source.map(lambda value: int(value) * 2, str, int)
    second = source.map(lambda value: int(value) * 3, str, int)
    assert first.collect() == [2, 4, 6]
    assert second.collect() == [3, 6, 9]
    assert first.collect() == [2, 4, 6]


def test_text_source(context, tmp_path):
    path = tmp_path / "input.txt"
    path.write_text("1\n2\n3\n", encoding="utf-8")
    assert context.textfile(path.as_uri()).collect() == ["1", "2", "3"]
    assert (context.textfile(path.as_uri())
            .map(lambda value: int(value) * 2, str, int).collect()) == [2, 4, 6]


def test_native_cartesian_pairs(context):
    result = (context.load_collection([1, 2])
              .cartesian(context.load_collection(["a", "b"])).collect())
    assert sorted(result) == [[1, "a"], [1, "b"], [2, "a"], [2, "b"]]


@pytest.mark.parametrize("value, message", [
    (None, "NoneType"), ({"error": "user data"}, "dict"), ({1}, "set"), (b"data", "bytes"),
    (object(), "object"), (math.nan, "finite"), (math.inf, "finite"),
    (2 ** 63, "signed 64 bits"),
])
def test_rejects_unsupported_udf_results(context, value, message):
    with pytest.raises(requests.HTTPError, match=message) as failure:
        context.load_collection([1]).map(lambda item: value, str, str).collect()
    assert failure.value.response.status_code == 400


def test_rejects_cyclic_udf_result(context):
    cycle = []
    cycle.append(cycle)
    with pytest.raises(requests.HTTPError, match="cyclic"):
        context.load_collection([1]).map(lambda value: cycle, str, str).collect()


def test_failure_discards_partial_results(context):
    with pytest.raises(requests.HTTPError, match="ZeroDivisionError") as failure:
        (context.load_collection([1, 2, 3])
         .map(lambda value: 1 / (int(value) - 2), str, float).collect())
    assert failure.value.response.status_code == 400
    assert "division by zero" in failure.value.response.text


@pytest.fixture
def native_plan():
    return {
        "context": {"origin": "scala", "platforms": ["java"], "configuration": {}},
        "operators": [{
            "id": 1, "operatorName": "inputCollection", "cat": "input",
            "input": [], "output": [], "data": {"values": [1, 2, 3]},
        }],
    }


@pytest.mark.parametrize("collect", [None, False, True])
@pytest.mark.parametrize("data", [{"values": [1, 2, 3]}, {"udf": "List(1, 2, 3)"}])
def test_legacy_and_collection_responses(api, native_plan, collect, data):
    native_plan["operators"][0]["data"] = data
    if collect is not None:
        native_plan["collect"] = collect
    response = requests.post(api, json=native_plan)
    assert response.status_code == 200, response.text
    if collect:
        assert response.headers["Content-Type"].startswith("application/json")
        assert response.json() == {"results": [1, 2, 3]}
    else:
        assert response.headers["Content-Type"].startswith("text/plain")
        assert response.text == "Buffer(1, 2, 3)"


def test_collect_rejects_sink_before_execution(api, native_plan, tmp_path):
    output = tmp_path / "must-not-exist.txt"
    native_plan["collect"] = True
    native_plan["operators"][0]["output"] = [2]
    native_plan["operators"].append({
        "id": 2, "operatorName": "textFileOutput", "cat": "output",
        "input": [1], "output": [], "data": {"filename": output.as_uri()},
    })
    response = requests.post(api, json=native_plan)
    assert response.status_code == 400
    assert "Collect plans cannot contain output sinks" in response.text
    assert not output.exists()
    del native_plan["collect"]
    response = requests.post(api, json=native_plan)
    assert response.status_code == 200, response.text
    assert response.text == "Success"
    assert output.read_text() == "1\n2\n3\n"


@pytest.mark.parametrize("terminals", [0, 2])
def test_rejects_ambiguous_terminals(api, native_plan, terminals):
    source = native_plan["operators"][0]
    native_plan["collect"] = True
    native_plan["operators"] = [dict(source, id=index + 1) for index in range(terminals)]
    response = requests.post(api, json=native_plan)
    assert response.status_code == 400
    assert "exactly one terminal operator" in response.text


def test_collect_on_spark(api):
    if os.environ.get("PYWAYANG_TEST_SPARK") != "1":
        pytest.skip("Set PYWAYANG_TEST_SPARK=1 with a backend configured for local Spark")
    configuration = Configuration()
    configuration.set_property("wayang.api.python.url", api)
    context = WayangContext(configuration).register({SparkPlugin})
    result = (context.load_collection([1, 2, 3])
              .map(lambda value: int(value) * 2, str, int).collect())
    assert sorted(result) == [2, 4, 6]
    assert all(type(value) is int for value in result)
    assert context.load_collection([1]).filter(lambda value: False, str).collect() == []
    assert context.load_collection([[1, [False, "雪"]], []]).collect() == [[1, [False, "雪"]], []]
