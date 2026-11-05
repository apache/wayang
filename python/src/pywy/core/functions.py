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
import json

from pywy.execution.util import JsonRecord
from pywy.operators.source import CollectionSource


class ChainedFunctions:
    def __init__(self):
        self.functions = []

    def add_function(self, function):
        self.functions.append(function)
        return self

    def execute(self, input_data):
        result = input_data
        for function in self.functions:
            result = function(result)
        return result

    def execute_json(self, input_data):
        # Encode before the worker's legacy str()/NumPy conversion loses types.
        try:
            for value in self.execute(input_data):
                value = CollectionSource._snapshot(value, set(), "collect result")
                yield JsonRecord(
                    json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
                )
        except Exception as error:
            # Preserve the Python cause instead of exposing only a socket EOF.
            yield JsonRecord(
                json.dumps({"error": f"{type(error).__name__}: {error}"}).encode("utf-8")
            )
