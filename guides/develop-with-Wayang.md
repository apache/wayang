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
This tutorial shows users how to import Wayang in their Java project using the maven.

# Include the Wayang maven dependencies in your pom
```shell
        <dependency>
            <groupId>org.apache.wayang</groupId>
            <artifactId>wayang-core</artifactId>
            <version>1.1.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.wayang</groupId>
            <artifactId>wayang-basic</artifactId>
            <version>1.1.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.wayang</groupId>
            <artifactId>wayang-java</artifactId>
            <version>1.1.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.wayang</groupId>
            <artifactId>wayang-spark</artifactId>
            <version>1.1.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.wayang</groupId>
            <artifactId>wayang-api-scala-java</artifactId>
            <version>1.1.1</version>
        </dependency>
```

# Include the Spark maven dependencies in your pom
```shell
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.12</artifactId>
            <version>${spark.version}</version>
        </dependency>
```

# Other maven dependencies to consider
```shell
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>2.12.0</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>1.7.13</version>
        </dependency>
```
A sample pom file can be found [here](pom-example.xml).

# Test WordCount
## Create a Java class that contains the main method that runs the Wordcount
Here is a sample implementation getting as input the filename (e.g., file:/Projects/Wayang/test.txt)

```shell
public static void main(String[] args) {

        /* Create a Wayang context and specify the platforms Wayang will consider */
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin());

        /* Get a plan builder */
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("WordCount")
                .withUdfJarOf(WordCount.class);

        /* Start building the Apache WayangPlan */
        Collection<Tuple2<String, Integer>> wordcounts = planBuilder
                /* Read the text file */
                .readTextFile(args[0]).withName("Load file")

                /* Split each line by non-word characters */
                .flatMap(line -> Arrays.asList(line.split("\\W+")))
                .withName("Split words")

                /* Filter empty tokens */
                .filter(token -> !token.isEmpty())
                .withName("Filter empty words")
                /* you can also specify the desired platform per operator */
                //.withTargetPlatform(Java.platform())

                /* Attach counter to each word */
                .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")

                /* Sum up counters for every word. */
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withName("Add counters")

                /* Execute the plan and collect the results */
                .collect();

        System.out.println(wordcounts);
}
```
A sample Java class file can be found [here](WordCount.java).


# More Wayang jobs
For more examples of Wayang jobs see [here](github.com/zkaoudi/wayang-examples).
### Running a Simple Program

After setting up Wayang, you can execute a simple program as follows:

```java
WayangContext context = new WayangContext(configuration);

JavaPlanBuilder planBuilder = new JavaPlanBuilder(context);

planBuilder
    .readTextFile("input.txt")
    .map(line -> line.toUpperCase())
    .writeTextFile("output.txt");

context.execute();
