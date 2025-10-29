# Apache Flink Cloudberry Connector

This repository contains the official Apache Flink Cloudberry connector.

## Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Cloudberry Database

Cloudberry Database is an open-source, next-generation unified analytics warehouse built on PostgreSQL. It provides powerful MPP (Massively Parallel Processing) capabilities for large-scale data analytics.

Learn more about Cloudberry Database at [https://cloudberry.apache.org/](https://cloudberry.apache.org/)

## Building the Apache Flink Cloudberry Connector from Source

Prerequisites:

* Unix-like environment (we use Linux, Mac OS X)
* Git
* Java 8 (Gradle wrapper will be downloaded automatically)

```bash
git clone https://github.com/yourusername/flink-connector-cloudberry.git
cd flink-connector-cloudberry
./gradlew clean build
```

To run only unit tests (fast):
```bash
./gradlew test
```

To run all tests (unit + integration):
```bash
./gradlew check
```

To skip integration tests:
```bash
./gradlew build -x integrationTest
```

The resulting jars can be found in the `build/libs` directory.

## Developing Flink

The Flink committers use IntelliJ IDEA to develop the Flink codebase.
We recommend IntelliJ IDEA for developing projects that involve Scala code.

Minimal requirements for an IDE are:
* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala

## Dependencies

**Important**: This is a core connector module. Users need to provide the PostgreSQL JDBC driver separately.

### Maven

```xml
<dependencies>
    <!-- Flink Cloudberry Connector -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-cloudberry</artifactId>
        <version>1.0-SNAPSHOT</version>
    </dependency>
    
    <!-- PostgreSQL JDBC Driver (Required!) -->
    <dependency>
        <groupId>org.postgresql</groupId>
        <artifactId>postgresql</artifactId>
        <version>42.7.3</version>
        <scope>runtime</scope>
    </dependency>
</dependencies>
```

### Gradle

```groovy
dependencies {
    implementation 'org.apache.flink:flink-connector-cloudberry:1.0-SNAPSHOT'
    runtimeOnly 'org.postgresql:postgresql:42.7.3'  // Required!
}
```

## Usage Example

### Table API

```java
// Create a Cloudberry catalog
TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

// Register Cloudberry sink table
tableEnv.executeSql(
    "CREATE TABLE cloudberry_sink (" +
    "  id BIGINT," +
    "  name STRING," +
    "  age INT" +
    ") WITH (" +
    "  'connector' = 'cloudberry'," +
    "  'url' = 'jdbc:postgresql://localhost:5432/testdb'," +
    "  'table-name' = 'users_output'," +
    "  'username' = 'your_username'," +
    "  'password' = 'your_password'" +
    ")"
);

// Execute query
tableEnv.executeSql("INSERT INTO cloudberry_sink SELECT * FROM cloudberry_source WHERE age > 18");
```

**Note**: Make sure the PostgreSQL JDBC driver is in your classpath!

## Documentation

The documentation of Apache Flink is located on the website: [https://flink.apache.org](https://flink.apache.org)
or in the `docs/` directory of the source code.

## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it.

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
