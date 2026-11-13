<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
--->

# PyWayang

Implementation of a python API for Apache Wayang

## Building from source
To build and install `pywy` as a python library, `build` is needed. It
can be installed using:
```shell
cd ./python
pip install --upgrade build
```
Installing this might require `python3.8-venv` to be installed on the
system.

After building the python package, execute the following steps to make
it available for your system:
```shell
python3 -m pip install dist/pywy-1.0.0.tar.gz
```

## Executing python code
In order to execute python code, the REST API needs to be running.
Compiling the assembly and executing the Main class for the REST API can
be done with the following steps:


Before compiling your code, make sure the required configuration variables
are set correctly in `wayang-api/wayang-api-python/src/main/resources/wayang-api-python-defaults.properties`.
This example `wayang-api-python-defaults.properties` can be used as a
guideline:

```
wayang.api.python.worker = /var/www/html/python/src/pywy/execution/worker.py
wayang.api.python.path = python3
wayang.api.python.env.path = /usr/local/lib/python3.8/dist-packages
```
The first configuration value needs to point to the location of your
apache wayang repository so that it can find the python worker that
executes UDFs (usually your current work directory +
`python/src/pypy/execution/worker.py`)
The second value is your command used for invoking python scripts.
Usually either python3 or just python.
The third value points to the directory in which python libraries are to
be found (usually where pip installs them).

- Package the project
```shell
./mvnw clean package -pl :wayang-assembly -Pdistribution
```

- Starting the REST API as a background process
```shell
cd wayang-assembly/target/
tar -xvf apache-wayang-assembly-1.0.0-incubating-dist.tar.gz
cd wayang-1.0.0
./bin/wayang-submit org.apache.wayang.api.json.Main &
```

Now, create and execute a python script like this:

```python
from pywy.dataquanta import WayangContext
from pywy.platforms.java import JavaPlugin
from pywy.platforms.spark import SparkPlugin

def word_count():
    ctx = WayangContext() \
        .register({JavaPlugin, SparkPlugin}) \
        .textfile("file:///README.md") \
        .flatmap(lambda w: w.split()) \
        .filter(lambda w: w.strip() != "") \
        .map(lambda w: (w.lower(), 1)) \
        .reduce_by_key(lambda t: t[0], lambda t1, t2: (t1[0], int(t1[1]) + int(t2[1]))) \
        .store_textfile("file:///wordcount-out-python.txt")

if __name__ == "__main__":
    word_count
```

By default, `pywy` submits plans to
`http://localhost:8080/wayang-api-json/submit-plan/json`. For a remote REST API
or another container, configure either the full URL:

```python
from pywy.configuration import Configuration
from pywy.dataquanta import WayangContext

configuration = Configuration()
configuration.set_property("wayang.api.python.url", "http://wayang-rest:8080/wayang-api-json/submit-plan/json")
ctx = WayangContext(configuration)
```

or configure host and port separately:

```python
configuration.set_property("wayang.api.python.host", "wayang-rest")
configuration.set_property("wayang.api.python.port", "8080")
```

The same values can be supplied through environment variables:
`WAYANG_API_URL`, or `WAYANG_API_HOST` and `WAYANG_API_PORT`.

## In-memory collections

`load_collection()` creates a `DataQuanta` from a Python list, tuple, or NumPy
array. It copies the input, preserving order and nesting; empty inputs are valid.
Supported elements are strings, booleans, signed 64-bit integers, finite floats,
and nested lists, tuples, or arrays. NumPy boolean, integer, float16/32/64, and
Unicode values are supported within the same limits. Unsupported values raise
`TypeError` or `ValueError` before execution.

```python
import numpy as np
from pywy.dataquanta import WayangContext
from pywy.platforms.java import JavaPlugin

ctx = WayangContext().register({JavaPlugin})
ctx.load_collection([1, 2, 3]).store_textfile("file:///tmp/numbers.txt")
ctx.load_collection(np.array([1.5, 2.5])).store_textfile("file:///tmp/floats.txt")
```

## Docker

The Python API can be run with the Wayang REST API as a separate container.
Build the assembly first, then build the base Wayang image and the optional
Python-enabled image from the repository root:

```shell
./mvnw clean install -Dmaven.test.skip=true
./mvnw package -pl :wayang-assembly -Pdistribution -Dmaven.test.skip=true
docker buildx build --load -t apache-wayang:ci .
docker buildx build --load -f python/Dockerfile -t apache-wayang-python:ci .
```

Start the REST API and run the Python example on the same Docker network:

```shell
docker network create wayang-python
docker run -d --name wayang-rest --network wayang-python apache-wayang-python:ci
docker run --rm --network wayang-python \
  --entrypoint /usr/local/bin/wayang-python-entrypoint \
  -e WAYANG_API_HOST=wayang-rest \
  apache-wayang-python:ci \
  python python/examples/wordcount.py
docker rm -f wayang-rest
docker network rm wayang-python
```

### Testing python code 
You can run the python tests by using pytest, the requirements for the tests are listed in `python/src/pywy/requirements.txt`. To run the tests navigate to the base wayang folder, e.g. `/var/www/html` and run `pytest -s python/src/pywy` if you need to pass a specific configuration for your use case you can also add a config flag `pytest -s --config=pathToYourConfig python/src/pywy/`

Collection unit tests run without a backend. See the setup instructions in
[`test_load_collection_integration.py`](src/pywy/tests/test_load_collection_integration.py)
for running the collection tests against a local REST backend.
