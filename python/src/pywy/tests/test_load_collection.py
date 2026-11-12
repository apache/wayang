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
import json
from unittest.mock import patch

import cloudpickle
import pytest

from pywy.dataquanta import DataQuanta, WayangContext
from pywy.operators import CollectionSource, SourceUnaryOperator
from pywy.platforms.java import JavaPlugin


@pytest.mark.parametrize("values", [
    [1, -2, 0, -(2 ** 63), 2 ** 63 - 1],
    [1.5, -0.0, 1e-300, 1e300],
    ['quotes " and \\', "line\nfeed\ttab", "雪 😀", "", '${System.exit(1)}'],
    [],
    [True, False],
    [[1, 2], [], [3, ["nested", False]]],
    [1, "two", 3.5, True],
])
def test_collection_plan(values):
    context = WayangContext().register({JavaPlugin})
    data = context.load_collection(values)
    assert isinstance(data, DataQuanta)
    assert data.context is context
    assert isinstance(data.operator, SourceUnaryOperator)
    assert data.operator.inputs == 0
    assert data.operator.outputs == 1

    with patch("pywy.core.core.requests.post") as post:
        data.store_textfile("file:///tmp/collection.txt")

    plan = json.loads(json.dumps(post.call_args.kwargs["json"], allow_nan=False))
    assert plan["context"]["origin"] == "python"
    assert plan["context"]["platforms"] == ["java"]
    source, sink = plan["operators"]
    assert source == {
        "id": 1, "operatorName": "inputCollection", "cat": "input",
        "input": [], "output": [2], "data": {"values": values},
    }
    assert sink["operatorName"] == "textFileOutput"
    assert sink["input"] == [1]
    assert sink["data"]["filename"] == "file:///tmp/collection.txt"


def test_snapshot_and_tuples():
    nested = [1, (2, 3)]
    values = [nested, nested]
    source = CollectionSource(values)
    nested.append(4)
    values.clear()
    assert source.values == [[1, [2, 3]], [1, [2, 3]]]
    assert CollectionSource((1, 2)).values == [1, 2]


@pytest.mark.parametrize("values", ["abc", b"abc", {1, 2}, {"a": 1}, None, iter([1, 2])])
def test_reject_unordered_or_non_collection_inputs(values):
    with pytest.raises(TypeError, match="expects a list or tuple"):
        WayangContext().load_collection(values)


@pytest.mark.parametrize("value", [None, {}, {1}, b"bytes", complex(1, 2), object()])
def test_reject_unsupported_values(value):
    with pytest.raises(TypeError, match=r"collection\[0\]\[0\]: unsupported"):
        WayangContext().load_collection([[value]])


@pytest.mark.parametrize("value", [-(2 ** 63) - 1, 2 ** 63, float("nan"), float("inf"), -float("inf")])
def test_reject_out_of_range_values(value):
    with pytest.raises(ValueError, match=r"collection\[0\]"):
        WayangContext().load_collection([value])


def test_reject_cycles():
    values = []
    values.append(values)
    with pytest.raises(ValueError, match="cyclic"):
        WayangContext().load_collection(values)


def test_collection_transformation():
    with patch("pywy.core.core.requests.post") as post:
        (WayangContext().register({JavaPlugin}).load_collection([3, 1, 2])
         .filter(lambda value: int(value) > 1, str)
         .map(lambda value: int(value) * 2, str, int)
         .store_textfile("file:///tmp/doubled.txt"))
    source, transform, sink = post.call_args.kwargs["json"]["operators"]
    assert source["output"] == [transform["id"]]
    assert transform["input"] == [source["id"]]
    assert transform["output"] == [sink["id"]]
    assert sink["input"] == [transform["id"]]
    assert transform["operatorName"] == "mapPartitions"
    udf = cloudpickle.loads(base64.b64decode(transform["data"]["udf"]))
    assert list(udf(iter(["3", "1", "2"]))) == [6, 4]
