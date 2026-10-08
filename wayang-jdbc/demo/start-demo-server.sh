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
demo_work_dir="${TMPDIR:-/tmp}/wayang-jdbc-demo"
demo_properties="${demo_work_dir}/wayang.properties"
data_dir="${script_dir}/data"
model_data_dir="/${data_dir}"
demo_properties_url="file:${demo_properties}"
server_classpath_file="${repo_root}/wayang-jdbc/wayang-jdbc-server/target/runtime-classpath.txt"
server_jar="${repo_root}/wayang-jdbc/wayang-jdbc-server/target/apache-wayang-jdbc-server-1.1.2-SNAPSHOT.jar"

mkdir -p "${demo_work_dir}"

cat > "${demo_properties}" <<EOF
wayang.calcite.model={"version":"1.0","defaultSchema":"fs","schemas":[{"name":"fs","type":"custom","factory":"org.apache.calcite.adapter.file.FileSchemaFactory","operand":{"directory":"${model_data_dir}"}}]}
wayang.ml.experience.enabled=false
wayang.ml.executions.file=${demo_work_dir}/mle.txt
wayang.ml.optimizations.file=${demo_work_dir}/mlo.txt
EOF

cd "${repo_root}"

echo "Building Wayang JDBC server artifacts..."
./mvnw -pl :wayang-jdbc-server -am -DskipTests -Dmaven.javadoc.skip=true install

echo "Creating server runtime classpath..."
./mvnw -pl :wayang-jdbc-server -DincludeScope=runtime -Dmdep.outputFile=target/runtime-classpath.txt dependency:build-classpath

echo "Demo configuration written to ${demo_properties}"
echo "Wayang configuration URL: ${demo_properties_url}"
echo "Starting Wayang JDBC server on 127.0.0.1:9999"
echo "Leave this terminal running. In another terminal, run:"
echo "  bash wayang-jdbc/demo/run-demo-client.sh"

java -cp "${server_jar}:$(cat "${server_classpath_file}")" \
    org.apache.wayang.jdbc.server.WayangJdbcServer \
    127.0.0.1 \
    9999 \
    "${demo_properties_url}"
