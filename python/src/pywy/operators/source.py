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

import math
from typing import List, Optional

import numpy as np

from pywy.operators.base import PywyOperator
from pywy.basic.data.record import Record
from pywy.types import GenericTco


class SourceUnaryOperator(PywyOperator):

    def __init__(self, name: str, output_type: GenericTco):
        super(SourceUnaryOperator, self).__init__(
            name=name,
            input_type=None,
            output_type=output_type,
            cat="input",
            input_length=0,
            output_length=1
        )

    def postfix(self) -> str:
        return 'Source'

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class CollectionSource(SourceUnaryOperator):
    def __init__(self, collection):
        # Empty and heterogeneous collections use the backend's generic type.
        super().__init__('Collection', output_type=None)
        self.json_name = "inputCollection"
        if type(collection) not in (list, tuple, np.ndarray):
            raise TypeError("load_collection expects a list, tuple, or NumPy array")
        if type(collection) is np.ndarray and collection.ndim == 0:
            raise TypeError("load_collection expects a NumPy array with at least one dimension")
        self.values = self._snapshot(collection, set(), "collection")

    @staticmethod
    def _snapshot(value, ancestors, path):
        # Apply the same JSON type and range checks after NumPy conversion.
        if isinstance(value, np.generic) and value.dtype.kind in "biufU":
            value = value.item()
        if type(value) is np.ndarray:
            if value.dtype.kind not in "biufUO" or (value.dtype.kind == "f" and value.dtype.itemsize > 8):
                raise TypeError(f"{path}: unsupported NumPy dtype {value.dtype}")
        if type(value) in (str, bool):
            return value
        if type(value) is int:
            if not -(2 ** 63) <= value < 2 ** 63:
                raise ValueError(f"{path}: integers must fit in signed 64 bits")
            return value
        if type(value) is float:
            if not math.isfinite(value):
                raise ValueError(f"{path}: floats must be finite")
            return value
        if type(value) in (list, tuple, np.ndarray):
            if id(value) in ancestors:
                raise ValueError(f"{path}: cyclic collections are not supported")
            ancestors.add(id(value))
            items = value.tolist() if type(value) is np.ndarray else value
            if type(value) is np.ndarray and value.ndim == 0:
                result = CollectionSource._snapshot(items, ancestors, path)
            else:
                result = [
                    CollectionSource._snapshot(item, ancestors, f"{path}[{index}]")
                    for index, item in enumerate(items)
                ]
            ancestors.remove(id(value))
            return result
        raise TypeError(f"{path}: unsupported collection value of type {type(value).__name__}")


class TextFileSource(SourceUnaryOperator):
    path: str
    json_name: str

    def __init__(self, path: str):
        super(TextFileSource, self).__init__('TextFile', output_type=str)
        self.path = path
        self.json_name = "textFileInput"

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class ParquetSource(SourceUnaryOperator):
    path: str
    projection: Optional[List[str]]
    json_name: str

    def __init__(self, path: str, projection: Optional[List[str]] = None):
        super(ParquetSource, self).__init__('Parquet', output_type=Record)
        self.path = path
        self.projection = projection
        self.json_name = "parquetInput"

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()
