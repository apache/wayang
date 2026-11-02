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

from typing import Set, List, Optional, Tuple, Union, cast

from pywy.core.core import Plugin, PywyPlan
from pywy.operators.base import PO_T
from pywy.types import (GenericTco, Predicate, Function, BiFunction, FlatmapFunction, IterableOut, T, In, Out)
from pywy.operators import *
from pywy.basic.data.record import Record
from pywy.basic.model.option import Option
from pywy.basic.model.models import (Model, LogisticRegression, DecisionTreeRegression, LinearSVC)
from pywy.configuration import Configuration

class WayangContext:
    """
    This is the entry point for users to work with Wayang.
    """
    plugins: Set[Plugin]
    configuration: Configuration

    def __init__(self, configuration: Configuration = Configuration()):
        self.plugins = set()
        self.configuration = configuration

    """
    add a :class:`Plugin` to the :class:`Context`
    """
    def register(self, *plugins: Plugin):
        for p in plugins:
            self.plugins.update(p)
        return self

    """
    remove a :class:`Plugin` from the :class:`Context`
    """
    def unregister(self, *plugins: Plugin):
        for p in plugins:
            self.plugins.remove(p)
        return self

    def textfile(self, file_path: str) -> "DataQuanta[str]":
        return DataQuanta(self, TextFileSource(file_path))

    def load_collection(self, collection: Union[List[T], Tuple[T, ...]]) -> "DataQuanta[T]":
        """Create a source from a snapshot of an ordered list or tuple.

        Values may be strings, booleans, signed 64-bit integers, finite floats,
        or nested lists/tuples (tuples become lists). Empty collections are valid.
        Other values, including None, mappings, sets, and cyclic containers,
        raise TypeError or ValueError before a plan is submitted.

        As with other sources, the current Python worker passes records to UDFs
        as strings. Numeric UDFs must convert their input explicitly with int()
        or float(). See the README for a fused transformation example and
        existing worker limitations.
        """
        return DataQuanta(self, CollectionSource(collection))

    def parquet(
        self, file_path: str, projection: Optional[List[str]] = None, column_names: Optional[List[str]] = None
    ) -> "DataQuanta[Record]":
        return DataQuanta(self, ParquetSource(file_path, projection, column_names))

    def __str__(self):
        return "Plugins: {}".format(str(self.plugins))

    def __repr__(self):
        return self.__str__()


class DataQuanta(GenericTco):
    """
    Represents an intermediate result/data flow edge in a [[WayangPlan]].
    """
    context: WayangContext

    def __init__(self, context: WayangContext, operator: PywyOperator):
        self.operator = operator
        self.context = context

    def filter(self: "DataQuanta[T]", p: Predicate, input_type: GenericTco = None) -> "DataQuanta[T]":
        return DataQuanta(self.context, self._connect(FilterOperator(p, input_type)))

    def map(
        self: "DataQuanta[In]",
        f: Function,
        input_type: GenericTco = None,
        output_type: GenericTco = None
    ) -> "DataQuanta[Out]":
        return DataQuanta(self.context, self._connect(MapOperator(f, input_type, output_type)))

    def flatmap(
        self: "DataQuanta[In]",
        f: FlatmapFunction,
        input_type: GenericTco = None,
        output_type: GenericTco = None
    ) -> "DataQuanta[IterableOut]":
        return DataQuanta(self.context, self._connect(FlatmapOperator(f, input_type, output_type)))

    def reduce_by_key(
        self: "DataQuanta[In]",
        key_f: Function,
        f: BiFunction,
        input_type: GenericTco = None
    ) -> "DataQuanta[IterableOut]":
        return DataQuanta(self.context, self._connect(ReduceByKeyOperator(key_f, f, input_type)))

    def sort(self: "DataQuanta[In]", key_f: Function, input_type: GenericTco = None) -> "DataQuanta[IterableOut]":
        return DataQuanta(self.context, self._connect(SortOperator(key_f, input_type)))

    def join(
        self: "DataQuanta[In]",
        this_key_f: Function,
        that: "DataQuanta[In]",
        that_key_f: Function,
        input_type: GenericTco = None,
    ) -> "DataQuanta[Out]":
        op = JoinOperator(
            this_key_f,
            that,
            that_key_f,
            input_type
        )

        self._connect(op),
        return DataQuanta(
            self.context,
            that._connect(op, 1)
        )

    def cartesian(
        self: "DataQuanta[In]",
        that: "DataQuanta[In]",
        input_type: GenericTco = None,
    ) -> "DataQuanta[Out]":
        op = CartesianOperator(
            that,
            input_type
        )

        self._connect(op),
        return DataQuanta(
            self.context,
            that._connect(op, 1)
        )

    def dlTraining(
        self: "DataQuanta[In]",
        model: Model,
        option: Option,
        that: "DataQuanta[In]",
        input_type: GenericTco,
        output_type: GenericTco
    ) -> "DataQuanta[Out]":
        op = DLTrainingOperator(
            model,
            option,
            input_type,
            output_type
        )
        self._connect(op)

        return DataQuanta(
            self.context,
            that._connect(op, 1)
        )

    def predict(
        self: "DataQuanta[In]",
        that: "DataQuanta[In]",
        input_type: GenericTco,
        output_type: GenericTco
    ) -> "DataQuanta[Out]":
        op = PredictOperator(
            input_type,
            output_type
        )

        self._connect(op)

        return DataQuanta(
            self.context,
            that._connect(op, 1)
        )


    def train_logistic_regression(
            self: "DataQuanta[In]",
            labels: "DataQuanta[In]",
            fit_intercept: bool = True
    ) -> "DataQuanta[Out]":
        op = LogisticRegression()
        self._connect(op, 0)
        labels._connect(op, 1)
        return DataQuanta(self.context, op)


    def train_decision_tree_regression(
            self: "DataQuanta[In]",
            labels: "DataQuanta[In]",
            max_depth: int = 5,
            min_instances: int = 2
    ) -> "DataQuanta[Out]":
        op = DecisionTreeRegression(max_depth, min_instances)
        self._connect(op, 0)
        labels._connect(op, 1)
        return DataQuanta(self.context, op)

    def train_linear_svc(
            self: "DataQuanta[In]",
            labels: "DataQuanta[In]",
            max_iter: int = 10,
            reg_param: float = 0.1
    ) -> "DataQuanta[Out]":
        op = LinearSVC(max_iter=max_iter, reg_param=reg_param)
        self._connect(op, 0)
        labels._connect(op, 1)
        return DataQuanta(self.context, op)



    def store_textfile(self: "DataQuanta[In]", path: str, input_type: GenericTco = None) -> None:
        last: List[SinkOperator] = [
            cast(
                SinkOperator,
                self._connect(
                    TextFileSink(
                        path,
                        input_type
                    )
                )
            )
        ]
        PywyPlan(self.context.plugins, self.context.configuration, last).execute()

    def _connect(self, op: PO_T, port_op: int = 0) -> PywyOperator:
        self.operator.connect(0, op, port_op)
        return op

    def __str__(self):
        return str(self.operator)

    def __repr__(self):
        return self.__str__()
