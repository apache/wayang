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

# Hackathon developer environment (Community over Code 2026, Glasgow).
#
# This is a JDK-based development container, not a slim runtime image:
# participants are expected to edit Wayang source and rebuild Wayang during
# the hackathon, so we keep Maven Wrapper + git + the full source tree
# available inside it.
#
# Build context is the REPO ROOT (see compose.yaml: `context: ..`), because
# the whole source tree needs to be present to build Wayang.

FROM eclipse-temurin:17-jdk

LABEL org.opencontainers.image.source="https://github.com/apache/wayang"

# git      -> some mvnw/plugin steps and contributor workflows expect it
# bash     -> mvnw and our entrypoint are bash scripts
# postgresql-client -> lets participants run `psql` from inside the
#                       container to poke at the hackathon Postgres service
RUN apt-get update \
    && apt-get install -y --no-install-recommends git bash postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace/wayang

# The source tree is normally supplied at runtime via the bind mount in
# compose.yaml (so local edits show up immediately). We still COPY it here
# so `docker build` works standalone, without compose, e.g. in CI.
COPY . /workspace/wayang

# The repo's Maven Wrapper is frequently checked out with CRLF line endings
# on Windows, which breaks execution inside the Linux container.
RUN sed -i 's/\r$//' mvnw \
    && chmod +x mvnw

COPY hackathon-CoC2026/docker/entrypoint.sh /usr/local/bin/wayang-entrypoint
RUN chmod +x /usr/local/bin/wayang-entrypoint

# Pre-warm the Maven local repo (~/.m2) at image build time, so a PUSHED,
# PULLED image already has every dependency downloaded — this is what
# actually gets participants under the 10-15 minute target, vs. a
# from-source build which takes much longer on a cold cache (see
# README.md "Timing"). This step builds against whatever source was
# checked out when this image was built.
#
# ~/.m2 lives outside /workspace/wayang, so it is NOT overwritten by the
# bind mount in compose.yaml. The first time the wayang-maven-cache named
# volume is created, Docker seeds it from this image layer automatically
# (standard behavior for a volume mounted over a non-empty image
# directory), so the warm cache survives into the running container.
RUN ./mvnw -B -q clean package -pl :wayang-assembly -Pdistribution -DskipTests \
    && ./mvnw -B -q dependency:build-classpath -pl :wayang-assembly \
       -Dmdep.outputFile=/tmp/wayang-classpath.txt \
    && rm -rf /workspace/wayang/wayang-assembly/target

ENTRYPOINT ["wayang-entrypoint"]
CMD ["bash"]
