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

RC=rc8
VERSION=0.6.0
URL=https://dist.apache.org/repos/dist/dev/wayang
NAME=apache-wayang-${VERSION}
NAME_SRC_BASE=${NAME}-source-release
NAME_SRC_EXT=${NAME_SRC_BASE}.zip

if [[ -d "validate" ]]; then
  rm -rf validate
fi

mkdir validate
cd validate

#RAT
echo "============================================================================================="
echo "==============     Download RAT                                        ======================"
echo "============================================================================================="
wget https://dlcdn.apache.org//creadur/apache-rat-0.13/apache-rat-0.13-bin.zip &>/dev/null
unzip -qq apache-rat-0.13-bin.zip

#Download files
echo "" ; echo ""
echo "============================================================================================="
echo "==============     Download files                                      ======================"
echo "============================================================================================="
wget ${URL}/${VERSION}/${RC}/DISCLAIMER &>/dev/null
wget ${URL}/${VERSION}/${RC}/LICENSE &>/dev/null
wget ${URL}/${VERSION}/${RC}/NOTICE &>/dev/null
wget ${URL}/${VERSION}/${RC}/README.md &>/dev/null
wget ${URL}/${VERSION}/${RC}/RELEASE_NOTES &>/dev/null
wget ${URL}/${VERSION}/${RC}/${NAME_SRC_EXT} &>/dev/null
wget ${URL}/${VERSION}/${RC}/${NAME_SRC_EXT}.asc &>/dev/null
wget ${URL}/${VERSION}/${RC}/${NAME_SRC_EXT}.sha512 &>/dev/null


#Calculate hash of files
echo "" ; echo ""
echo "============================================================================================="
echo "==============     Calculate Hash                                      ======================"
echo "============================================================================================="
HASH_2_VALIDATE=$(cat ${NAME_SRC_EXT}.sha512 | sed -e '$a\')
HASH_CALCULATED=$(shasum -a 512 ${NAME_SRC_EXT} | cut -d ' ' -f 1 | tr -d '\n')
diff -is <( echo "${HASH_2_VALIDATE}") <(echo "${HASH_CALCULATED}")

#Calculate the gpg
echo "" ; echo ""
echo "============================================================================================="
echo "==============     Validate Sign                                       ======================"
echo "============================================================================================="
gpg --verify ${NAME_SRC_EXT}.asc ${NAME_SRC_EXT}

#Unzip files
echo "" ; echo ""
echo "============================================================================================="
echo "==============     Unzip files                                         ======================"
echo "============================================================================================="
unzip -qq ${NAME_SRC_EXT}

#validate license in the headers
echo "" ; echo ""
echo "============================================================================================="
echo "==============     Validate licenses in files                          ======================"
echo "============================================================================================="
java -jar ./apache-rat-0.13/apache-rat-0.13.jar ./${NAME} | grep "== File: "

#move the final folder
echo "" ; echo ""
echo "============================================================================================="
echo "==============     Compiling files                                     ======================"
echo "============================================================================================="
cd ./${NAME}



#Compile the code
mvn clean install &> ../logs

tail -n 100 ../logs

