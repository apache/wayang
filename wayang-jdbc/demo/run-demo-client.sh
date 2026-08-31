#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
demo_classes_dir="${repo_root}/target/wayang-jdbc-demo/classes"
data_dir="${script_dir}/data"
driver_classpath_file="${repo_root}/wayang-jdbc/wayang-jdbc-driver/target/runtime-classpath.txt"
driver_jar="${repo_root}/wayang-jdbc/wayang-jdbc-driver/target/apache-wayang-jdbc-driver-1.1.2-SNAPSHOT.jar"
jdbc_url="${1:-jdbc:wayang://127.0.0.1:9999/demo}"

cd "${repo_root}"

mkdir -p "${demo_classes_dir}"

echo "Building Wayang JDBC driver artifacts..."
./mvnw -pl :wayang-jdbc-driver -am -DskipTests -Dmaven.javadoc.skip=true install

echo "Creating driver runtime classpath..."
./mvnw -pl :wayang-jdbc-driver -DincludeScope=runtime -Dmdep.outputFile=target/runtime-classpath.txt dependency:build-classpath

echo "Compiling demo client..."
javac -cp "${driver_jar}:$(cat "${driver_classpath_file}")" \
    -d "${demo_classes_dir}" \
    "${script_dir}/CsvSelectionOperationsDemo.java"

echo "Running demo client..."
java -cp "${demo_classes_dir}:${driver_jar}:$(cat "${driver_classpath_file}")" \
    CsvSelectionOperationsDemo \
    "${data_dir}" \
    fs \
    "${jdbc_url}"
