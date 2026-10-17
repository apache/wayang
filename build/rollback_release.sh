#!/bin/bash
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

RC=rc9
RC_NEW=rc10
VERSION=0.6.0
URL=https://dist.apache.org/repos/dist/dev/wayang
NAME=apache-wayang-${VERSION}
NAME_SRC_BASE=${NAME}-source-release
NAME_SRC_EXT=${NAME_SRC_BASE}.zip
# TODO: validate the enviroment before to execute
# gpg, gem_home, java_home
git tag wayang-${VERSION}-${RC} wayang-${VERSION}
git tag -d wayang-${VERSION}
git push origin wayang-${VERSION}-${RC} :wayang-${VERSION}


git branch -m rel/${VERSION} rel/${VERSION}-${RC}
git push origin :rel/${VERSION}
git push origin rel/${VERSION}-${RC}:refs/heads/rel/${VERSION}-${RC}

mvn versions:set -DnewVersion=${VERSION}-SNAPSHOT
mvn versions:commit

git add .
git commit -m "[RELEASE][PREPARATION] rollback to the version ${VERSION}-snapshot"
git push origin

mvn clean release:clean release:prepare -P web-documentation
mvn release:perform

git branch rel/${VERSION} wayang-${VERSION}
git push -u origin rel/${VERSION}

mkdir ${RC_NEW}
cp target/checkout/target/${NAME_SRC_EXT} ${RC_NEW}/
cp target/checkout/target/${NAME_SRC_EXT}.asc ${RC_NEW}/
cp target/checkout/target/${NAME_SRC_EXT}.sha512 ${RC_NEW}/
#cp target/checkout/DISCLAIMER ${RC_NEW}/
#cp target/checkout/LICENSE ${RC_NEW}/
#cp target/checkout/NOTICE ${RC_NEW}/
cp target/checkout/RELEASE_NOTES ${RC_NEW}/
cp target/checkout/README.md ${RC_NEW}/

svn import -m "Apache Wayang ${RC_NEW}" ${RC_NEW} ${URL}/${VERSION}/${RC_NEW} --username bertty


