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
This tutorial will show users how to run the WordCount example locally with Wayang.

# Clone repository
```shell
git clone https://github.com/apache/wayang.git
```

# Create binaries
Running following commands to build Wayang and generate the tar.gz  
```shell
cd wayang
./mvnw clean package -pl :wayang-assembly -Pdistribution 
```
Then you can find the `wayang-assembly-1.1.2-SNAPSHOT-dist.tar.gz` under `wayang-assembly/target` directory.


# Prepare the environment
## Wayang
```shell
tar -xvf wayang-assembly-1.1.2-SNAPSHOT-dist.tar.gz
cd wayang-1.1.2-SNAPSHOT
```

In linux
```shell 
echo "export WAYANG_HOME=$(pwd)" >> ~/.bashrc
echo "export PATH=${PATH}:${WAYANG_HOME}/bin" >> ~/.bashrc
source ~/.bashrc
```
In MacOS
```shell 
echo "export WAYANG_HOME=$(pwd)" >> ~/.zshrc
echo "export PATH=${PATH}:${WAYANG_HOME}/bin" >> ~/.zshrc
source ~/.zshrc
```
## Others
- You need to install Apache Spark version 3 or higher. Don’t forget to set the `SPARK_HOME` environment variable.
- You need to install Apache Hadoop version 3 or higher. Don’t forget to set the `HADOOP_HOME` environment variable.

# Run the program

To execute the WordCount example with Apache Wayang, you need to execute your program with the 'wayang-submit' command:

```shell
cd wayang-1.1.2-SNAPSHOT
./bin/wayang-submit org.apache.wayang.apps.wordcount.Main java file://$(pwd)/README.md
```
## If you're using Java 17, add the following JVM flags:
Update your `wayang-submit` (`wayang-assembly/target/wayang-1.1.2-SNAPSHOT/bin/wayang-submit`) script (or command) with:

```shell 
eval "$RUNNER \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  $FLAGS -cp \"${WAYANG_CLASSPATH}\" $CLASS ${ARGS}"
```

Then you should be able to see outputs like this:

![img.png](../images/wordcount_result.png)

## Running SQL Queries in Wayang

Wayang provides support for executing SQL queries using its SQL API.

### Steps to execute SQL queries

1. Configure the Wayang Calcite model using the property `wayang.calcite.model`:

```java
Configuration configuration = new Configuration();
configuration.setProperty(
    "wayang.calcite.model",
    "{ \"version\": \"1.0\", \"defaultSchema\": \"MY_SCHEMA\", \"schemas\": [] }"
);
```

2. Create a `SqlContext` instance:

```java
SqlContext context = new SqlContext(configuration);
```

3. Execute your SQL query:

```java
Collection<Record> result = context.executeSql("SELECT * FROM my_table");

for (Record record : result) {
    System.out.println(record);
}
```

### Example: Running a SQL Query

Wayang allows executing SQL queries using the `SqlContext` API.

```java
import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.basic.data.Record;

import java.util.Collection;

public class Example {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        configuration.setProperty(
            "wayang.calcite.model",
            "{ \"version\": \"1.0\", \"defaultSchema\": \"MY_SCHEMA\", \"schemas\": [] }"
        );

        SqlContext context = new SqlContext(configuration);

        Collection<Record> result = context.executeSql("SELECT * FROM my_table");

        for (Record record : result) {
            System.out.println(record);
        }
    }
}
```
