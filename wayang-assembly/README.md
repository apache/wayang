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

# Wayang Assembly
This is an assembly module for Apache Wayang project.

It creates a single tar.gz file that includes all needed dependency of the project
except for the jars in the list

- org.apache.hadoop.*, those are supposed to be available from the deployed Hadoop cluster.

> Note: This module is off by default. To activate it specify the profile in the command line
-Pdistribution

> Note: If you need to build an assembly for a different version of Hadoop the
> hadoop-version system property needs to be set as in this example: `-Dhadoop.version=2.7.4` at the 
> maven command line


# Execution Profile Assembly

To execute the Wayang Assembly you need to execute the following command in the project root

```shell
./mvnw clean install -DskipTests 
./mvnw clean package -pl :wayang-assembly -Pdistribution
```
