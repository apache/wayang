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

from typing import Set, Iterable, Dict
import json
import os
import requests

from pywy.configuration import Configuration
from pywy.core.platform import Platform
from pywy.core.serializer import JSONSerializer
from pywy.graph.graph import WayangGraph
from pywy.graph.types import WGraphOfVec
from pywy.operators import PywyOperator, CollectionSource, UnaryToUnaryOperator


class Plugin:
    """ TODO: enrich this documentation
    A plugin contributes the following components to a :class:`Context`
    - mappings
    - channels
    - configurations
    In turn, it may require several :clas:`Platform`s for its operation.
    """

    platforms: Set[Platform]

    def __init__(
            self,
            platforms: Set[Platform]):
        self.platforms = platforms

    def __str__(self):
        return "Platforms: {}".format(str(self.platforms))

    def __repr__(self):
        return self.__str__()


class PywyPlan:
    """A PywyPlan consists of a set of :py:class:`pywy.operators.base.PywyOperator`

    the operator inside PywyPlan follow a Directed acyclic graph(DAG), and describe
    how the execution needs to be performed

    Attributes
    ----------
    graph : :py:class:`pywy.graph.graph.WayangGraph`
       Graph that describe the DAG, and it provides the iterable properties to
       the PywyPlan
    plugins : :obj:`set` of :py:class:`pywy.core.plugin.Plugin`
        plugins is the set of possible platforms that can be uses to execute
        the PywyPlan
    sinks : :py:class:`typing.Iterable` of :py:class:`pywy.operators.base.PywyOperator`
        Terminal operators used to build the graph. For collect(), the terminal
        operator need not be a sink.
    """
    graph: WayangGraph

    def __init__(self, plugins: Set[Plugin], configuration: Configuration, sinks: Iterable[PywyOperator]):
        """basic Constructor of PywyPlan

        this constructor set the plugins and sinks element, and it prepares
        everything for been executed

        Parameters
        ----------
        plugins
            Description of `plugins`.
        sinks
            Description of `sinks`.
        """
        self.plugins = plugins
        self.configuration = configuration
        self.sinks = sinks
        self.set_graph()

    def set_graph(self):
        """ it builds the :py:class:`pywy.graph.graph.WayangGraph` of the current PywyPlan
        """
        self.graph = WGraphOfVec(self.sinks)

    def execute(self, collect: bool = False):
        """ Transform the plan into topologies to group pipelines into one
        MapPartition operator
        Transform the plan into json objects to send it to Wayang
        """
        json_data = {}
        context = {}
        context["origin"] = "python"
        context["platforms"] = {}
        context["configuration"] = self.configuration.entries

        if len(self.plugins) > 0:
            context["platforms"] = list(map(lambda pl: next(iter(pl.platforms)).name, self.plugins))

        json_data["context"] = context
        json_data["operators"] = []
        if collect:
            json_data["collect"] = True

        nodes = []
        pipeline = []
        self.graph.traversal(self.graph.starting_nodes, lambda x, parent: nodes.append(x))
        id_table = {(obj.current[0]): index + 1 for index, obj in enumerate(nodes)}
        serializer = JSONSerializer(id_table)

        for node in nodes:
            operator = node.current[0]
            if isinstance(operator, UnaryToUnaryOperator):
                pipeline.append(operator)
            else:
                if len(pipeline) > 0:
                    json_data["operators"].append(serializer.serialize_pipeline(pipeline))

                json_data["operators"].append(serializer.serialize(operator))
                pipeline = []

        if pipeline:
            json_data["operators"].append(serializer.serialize_pipeline(pipeline, json_output=collect))

        api_url = self.configuration.get_property("wayang.api.python.url") or os.environ.get("WAYANG_API_URL")
        if api_url is None:
            host = (
                self.configuration.get_property("wayang.api.python.host")
                or os.environ.get("WAYANG_API_HOST")
                or "localhost"
            )
            port = (
                self.configuration.get_property("wayang.api.python.port")
                or os.environ.get("WAYANG_API_PORT")
                or 8080
            )
            api_url = f"http://{host}:{port}/wayang-api-json/submit-plan/json"

        headers = {'Content-type': 'application/json'}
        json_body = json.dumps(json_data)
        print(json_body)
        response = requests.post(api_url, headers=headers, json=json_data)
        if collect:
            if not response.ok:
                raise requests.HTTPError(
                    f"Wayang execution failed (HTTP {response.status_code}): {response.text}",
                    response=response
                )
            try:
                body = response.json()
            except ValueError as error:
                raise ValueError(
                    "Wayang collect expected a JSON response; check the backend version"
                ) from error
            if not isinstance(body, dict) or not isinstance(body.get("results"), list):
                raise ValueError("Wayang collect expected a JSON object with a results array")
            try:
                return CollectionSource._snapshot(body["results"], set(), "results")
            except (TypeError, ValueError) as error:
                raise ValueError(f"Unsupported Wayang collect response: {error}") from error
