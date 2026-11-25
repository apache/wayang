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

"""Run against a local backend using PYWAYANG_INTEGRATION_URL (see README)."""

import os

import pytest
import requests

from pywy.configuration import Configuration
from pywy.dataquanta import WayangContext
from pywy.platforms.java import JavaPlugin


@pytest.fixture
def collection_context(monkeypatch):
    url = os.environ.get("PYWAYANG_INTEGRATION_URL")
    if not url:
        pytest.skip("Set PYWAYANG_INTEGRATION_URL to run against a local Wayang backend")
    configuration = Configuration()
    configuration.set_property("wayang.api.python.url", url)
    post = requests.post

    def checked_post(*args, **kwargs):
        response = post(*args, **kwargs, timeout=120)
        assert response.status_code == 200, response.text
        assert response.text == "Success"
        return response

    # Execute real HTTP requests and check errors that PywyPlan currently ignores.
    monkeypatch.setattr("pywy.core.core.requests.post", checked_post)
    return WayangContext(configuration).register({JavaPlugin})


@pytest.mark.parametrize("values, expected", [
    ([1, -2, 0, -(2 ** 63), 2 ** 63 - 1], "1\n-2\n0\n-9223372036854775808\n9223372036854775807\n"),
    ([1.5, -0.25, -0.0, 1.23456789012345], "1.5\n-0.25\n-0.0\n1.23456789012345\n"),
    (['quotes " and \\', "雪 😀", "line\nfeed\ttab", "", '${System.exit(1)}'],
     'quotes " and \\\n雪 😀\nline\nfeed\ttab\n\n${System.exit(1)}\n'),
    ([], ""),
    ([True, False], "true\nfalse\n"),
    ([[1, 2], [], [3, ["nested", False]]], "[1, 2]\n[]\n[3, [nested, false]]\n"),
])
def test_collection_to_textfile(collection_context, tmp_path, values, expected):
    output = tmp_path / "collection.txt"
    collection_context.load_collection(values).store_textfile(output.as_uri())
    assert output.read_text(encoding="utf-8") == expected


@pytest.mark.parametrize("values, expected", [([1, 2, 3], ["3", "5", "7"]), ([], [])])
def test_collection_maps_to_textfile(collection_context, tmp_path, values, expected):
    output = tmp_path / "doubled.txt"
    (collection_context.load_collection(values)
     .map(lambda value: int(value) * 2, str, int)
     .map(lambda value: value + 1, int, int)
     .store_textfile(output.as_uri()))
    assert output.read_text(encoding="utf-8").splitlines() == expected


@pytest.mark.parametrize("values, expected", [([3, 1, 2], ["6", "4"]), ([], [])])
def test_collection_filter_map_to_textfile(collection_context, tmp_path, values, expected):
    output = tmp_path / "filtered.txt"
    (collection_context.load_collection(values)
     .filter(lambda value: int(value) > 1, str)
     .map(lambda value: int(value) * 2, str, int)
     .store_textfile(output.as_uri()))
    assert output.read_text(encoding="utf-8").splitlines() == expected
