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

from typing import Set, Iterable
import json
import base64
import cloudpickle
import requests
import subprocess
import time

from pywy.core.platform import Platform
from pywy.core.functions import ChainedFunctions
from pywy.graph.graph import WayangGraph
from pywy.graph.types import WGraphOfVec, NodeOperator, NodeVec
from pywy.types import get_java_type, NDimArray, ndim_from_type
from pywy.operators import SinkOperator, UnaryToUnaryOperator, SourceUnaryOperator


class JSONSerializer:
    id_table: Iterable[int]

    def __init__(self, id_table: Iterable[int]):
        self.id_table = id_table

    def serialize(self, operator):
        json_operator = {}
        json_operator["id"] = self.id_table[operator]
        json_operator["operatorName"] = operator.json_name
        json_operator["cat"] = operator.cat
        if operator.cat != "input":
            json_operator["input"] = list(map(lambda x: self.id_table[x], operator.inputOperator))
        else:
            json_operator["input"] = []

        if operator.cat != "output":
            json_operator["output"] = list(map(lambda x: self.id_table[x], operator.outputOperator))
        else:
            json_operator["output"] = []

        json_operator["data"] = {}

        if hasattr(operator, "input_type") and operator.input_type is not None:
            json_operator["data"]["inputType"] = ndim_from_type(operator.input_type).to_json()

        if hasattr(operator, "output_type") and operator.output_type is not None:
            json_operator["data"]["outputType"] = ndim_from_type(operator.output_type).to_json()

        if operator.json_name == "inputCollection":
            json_operator["data"]["values"] = operator.values

        elif operator.json_name == "filter":
            json_operator["data"]["udf"] = base64.b64encode(cloudpickle.dumps(operator.use_predicate)).decode('utf-8')

        elif operator.json_name == "reduceBy":
            json_operator["data"]["keyUdf"] = base64.b64encode(cloudpickle.dumps(operator.key_function)).decode('utf-8')
            json_operator["data"]["udf"] = base64.b64encode(cloudpickle.dumps(operator.reduce_function)).decode('utf-8')

        elif operator.json_name == "join":
            json_operator["data"]["thisKeyUdf"] = base64.b64encode(cloudpickle.dumps(operator.this_key_function)).decode('utf-8')
            json_operator["data"]["thatKeyUdf"] = base64.b64encode(cloudpickle.dumps(operator.that_key_function)).decode('utf-8')

        elif operator.json_name == "cartesian":
            del json_operator["data"]

        elif operator.json_name == "dlTraining":
            json_operator["data"]["model"] = {"modelType": "DLModel", "op": operator.model.get_out().to_dict()}
            json_operator["data"]["option"] = operator.option.to_dict()

        else:
            if hasattr(operator, "get_udf"):
                json_operator["data"]["udf"] = base64.b64encode(cloudpickle.dumps(operator.get_udf)).decode('utf-8')

            if hasattr(operator, "path"):
                json_operator["data"]["filename"] = operator.path

            if hasattr(operator, "projection"):
                json_operator["data"]["projection"] = operator.projection

            if hasattr(operator, "column_names"):
                json_operator["data"]["column_names"] = operator.column_names

        return json_operator

    def serialize_pipeline(self, pipeline):
        assert len(pipeline) > 0

        json_operator = {}
        json_operator["id"] = self.id_table[pipeline[0]]
        json_operator["operatorName"] = "mapPartitions"
        json_operator["cat"] = "unary"
        json_operator["data"] = {}
        json_operator["input"] = list(map(lambda x: self.id_table[x], pipeline[0].inputOperator))
        json_operator["output"] = list(map(lambda x: self.id_table[x], pipeline[-1].outputOperator))

        if len(pipeline) == 1:
            return self.serialize(pipeline[0])

        chainedUdfs = ChainedFunctions()

        for operator in pipeline:
            self.id_table[operator] = json_operator["id"]
            if hasattr(operator, "get_udf"):
                chainedUdfs.add_function(operator.get_udf)

        json_operator["data"]["udf"] = base64.b64encode(cloudpickle.dumps(chainedUdfs.execute)).decode('utf-8')

        return json_operator
