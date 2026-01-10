<!--

  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

-->
# Compiling Apache Wayang 

Apache Wayang has different dependencies, for compiling, it needs to add some profile in the compilation to enable maven works properly.

 ```shell
mvn clean compile
```

The line before is because the plugin the Antlr is not needed in all the modules, as well it has happened with Scala language.

When maven compiles one or more modules using those plugins in the compilation time, it needs to add.

The modules are:
- wayang-api-scala-java
- wayang-core <- Antlr
- wayang-iejoin
- wayang-spark
- wayang-profiler
- wayang-tests-integration


# Executing Coverage

```shell
mvn clean verify jacoco:report
```

the final report is placed on `./target/aggregate.exec/aggregate.exec`
