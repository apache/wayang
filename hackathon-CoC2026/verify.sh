#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Run this from inside the wayang dev container:
#   docker compose exec wayang bash hackathon-CoC2026/verify.sh
#
# Builds Wayang, extracts the distribution (which has both a jars/ and a
# libs/ directory), resolves the full Maven runtime classpath (needed
# because Spark's own runtime jars are NOT copied into the assembled
# distribution — only wayang-spark's own jar is), and compiles + runs
# PostgresSmokeTest.java against the combined classpath.

set -euo pipefail
cd "$(dirname "$0")/.."   # repo root
REPO_ROOT="$(pwd)"

DIST_TARBALL="$(find wayang-assembly/target -maxdepth 1 -name 'apache-wayang-assembly-*.tar.gz' | head -n 1)"

if [ -z "$DIST_TARBALL" ]; then
  echo "==> Building wayang-assembly (first run can take significantly longer than"
  echo "    15 minutes on a cold Maven cache — this is expected, see README.md)"
  ./mvnw -q clean package -pl :wayang-assembly -Pdistribution -DskipTests -B

  DIST_TARBALL="$(find wayang-assembly/target -maxdepth 1 -name 'apache-wayang-assembly-*.tar.gz' | head -n 1)"
fi

if [ -z "$DIST_TARBALL" ]; then
  echo "Could not find apache-wayang-assembly-*.tar.gz under wayang-assembly/target/" >&2
  exit 1
fi

echo "==> Extracting $DIST_TARBALL"
tar -xzf "$DIST_TARBALL" -C wayang-assembly/target
DIST_DIR="$(find wayang-assembly/target -maxdepth 1 -type d -name 'wayang-*' | head -n 1)"
export WAYANG_HOME="$(cd "$DIST_DIR" && pwd)"
export PATH="$PATH:$WAYANG_HOME/bin"
echo "WAYANG_HOME=$WAYANG_HOME"
# Confirmed layout: $WAYANG_HOME/{bin,conf,jars,libs}

echo "==> Verifying Java platform (bundled WordCount app)"
( cd "$WAYANG_HOME" && bin/wayang-submit org.apache.wayang.apps.wordcount.Main java "file://$REPO_ROOT/README.md" | tail -n 20 ) || true
# Uses bin/wayang-submit as-is: it already carries the Java 17 --add-opens/
# --add-exports flags Spark needs, so this script does not duplicate that
# logic for this check — only for the direct `java` call below, which
# doesn't go through wayang-submit.

if [ -s /tmp/wayang-classpath.txt ]; then
  echo "==> Reusing the Maven classpath baked into this image at build time"
else
  echo "==> Resolving the full Maven runtime classpath (covers Spark's transitive"
  echo "    dependencies, which live in the local Maven repo, not the distribution)"
  ./mvnw -q -pl wayang-assembly dependency:build-classpath \
    -Dmdep.outputFile=/tmp/wayang-classpath.txt -B
fi
MAVEN_CP="$(cat /tmp/wayang-classpath.txt)"

DIST_CP="$WAYANG_HOME/libs/*:$WAYANG_HOME/jars/*"
FULL_CP="${DIST_CP}:${MAVEN_CP}"

echo "==> Reusing the Java 17 module flags already defined in wayang-submit"
JAVA17_FLAGS="$(grep -o -- '--add-\(opens\|exports\)=[^ "'"'"']*' "$WAYANG_HOME/bin/wayang-submit" | tr '\n' ' ')"

echo "==> Compiling and running PostgresSmokeTest.java"
mkdir -p /tmp/hackathon-classes
javac -cp "$FULL_CP" -d /tmp/hackathon-classes "$REPO_ROOT/hackathon-CoC2026/examples/PostgresSmokeTest.java"
java $JAVA17_FLAGS -cp "$FULL_CP:/tmp/hackathon-classes" PostgresSmokeTest
