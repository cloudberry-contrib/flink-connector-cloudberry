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
git clone https://github.com/cloudberry-contrib/flink-connector-cloudberry.git
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

## Dependencies

**Important**: This is a core connector module. Users need to provide the PostgreSQL JDBC driver separately.

### Maven

```xml
<dependencies>
    <!-- Flink Cloudberry Connector -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-cloudberry</artifactId>
        <version>1.0.0-SNAPSHOT</version>
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
    implementation 'org.apache.flink:flink-connector-cloudberry:1.0.0-SNAPSHOT'
    runtimeOnly 'org.postgresql:postgresql:42.7.3'  // Required!
}
```

## Usage Examples

### Quick Start - Table API

```sql
-- Register Cloudberry sink table
CREATE TABLE cloudberry_sink (
  id BIGINT,
  name STRING,
  age INT
) WITH (
  'connector' = 'cloudberry',
  'url' = 'jdbc:postgresql://localhost:5432/testdb',
  'table-name' = 'users_output',
  'username' = 'your_username',
  'password' = 'your_password'
);
```

**Note**: Make sure the PostgreSQL JDBC driver is in your classpath!

### Advanced Usage Examples

#### 1. DataStream API - Basic Insert with JdbcSink

Use DataStream API when you need fine-grained control over data processing:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);

// Create data stream and write to Cloudberry
env.fromElements(books)
    .addSink(
        JdbcSink.sink(
            // SQL insert statement
            "INSERT INTO books (id, title, author, price, quantity) VALUES (?, ?, ?, ?, ?)",
            // Statement builder - binds Java objects to SQL parameters
            (ps, book) -> {
                ps.setInt(1, book.id);
                ps.setString(2, book.title);
                ps.setString(3, book.author);
                ps.setDouble(4, book.price);
                ps.setInt(5, book.quantity);
            },
            // Connection options
            JdbcConnectionOptions.builder()
                .setJdbcUrl("jdbc:postgresql://localhost:5432/testdb")
                .setUsername("username")
                .setPassword("password")
                .setTable("books")
                .build()));

env.execute("Insert Books");
```

#### 2. Table API - Append Mode (Streaming)

Use append mode for write-only scenarios without updates:

```sql
-- Create sink table
CREATE TABLE order_sink (
  order_id INT,
  user_name STRING,
  amount DECIMAL(10,2),
  order_time TIMESTAMP(3)
) WITH (
  'connector' = 'cloudberry',
  'url' = 'jdbc:postgresql://localhost:5432/testdb',
  'username' = 'username',
  'password' = 'password',
  'table-name' = 'user_orders'
);

```

#### 3. Table API - Upsert Mode (with Primary Key)

Use upsert mode for aggregations and scenarios requiring updates:

```sql
-- Create sink table with primary key - enables upsert mode automatically
CREATE TABLE stats_sink (
  user_id INT,
  total_orders BIGINT,
  total_amount DECIMAL(18,2),
  last_update TIMESTAMP(3),
  PRIMARY KEY (user_id) NOT ENFORCED  -- Primary key enables upsert!
) WITH (
  'connector' = 'cloudberry',
  'url' = 'jdbc:postgresql://localhost:5432/testdb',
  'username' = 'username',
  'password' = 'password',
  'table-name' = 'user_statistics',
  'sink.buffer-flush.max-rows' = '2',
  'sink.buffer-flush.interval' = '0'
);

```

#### 4. High-Performance Bulk Loading with COPY Protocol

Use COPY protocol for high-performance bulk data loading (much faster than standard INSERT):

```sql
-- Enable COPY protocol for faster bulk inserts
CREATE TABLE bulk_orders_sink (
  order_id INT,
  product_name STRING,
  quantity INT,
  total_price DECIMAL(10,2),
  order_date TIMESTAMP(3)
) WITH (
  'connector' = 'cloudberry',
  'url' = 'jdbc:postgresql://localhost:5432/testdb',
  'username' = 'username',
  'password' = 'password',
  'table-name' = 'bulk_orders',
  'sink.use-copy-protocol' = 'true',        -- Enable COPY protocol
  'sink.buffer-flush.max-rows' = '100',     -- Flush after 100 rows
  'sink.buffer-flush.interval' = '1s'       -- Or flush every 1 second
);

```

#### 5. Batch Mode Execution

Use batch mode for offline analytics and ETL jobs:

```sql
-- Create sink table
CREATE TABLE sales_sink (
  product_name STRING,
  sales_count BIGINT
) WITH (
  'connector' = 'cloudberry',
  'url' = 'jdbc:postgresql://localhost:5432/testdb',
  'username' = 'username',
  'password' = 'password',
  'table-name' = 'product_sales',
  'sink.buffer-flush.max-rows' = '3',
  'sink.buffer-flush.interval' = '300ms'
);

```

## Configuration Options

| Option | Required | Default | Type | Description |
|--------|----------|---------|------|-------------|
| `connector` | Yes | - | String | Must be 'cloudberry' |
| `url` | Yes | - | String | JDBC connection URL (e.g., `jdbc:postgresql://host:port/database`) |
| `table-name` | Yes | - | String | Target table name in the database |
| `username` | No | - | String | Database username |
| `password` | No | - | String | Database password |
| `driver` | No | Auto-detected | String | JDBC driver class name |
| `sink.use-copy-protocol` | No | false | Boolean | Enable PostgreSQL COPY protocol for high-performance bulk loading |
| `sink.buffer-flush.max-rows` | No | 5000 | Integer | Maximum number of rows to buffer before flushing |
| `sink.buffer-flush.interval` | No | 0 | Duration | Flush interval (e.g., '1s', '500ms'). 0 means no interval-based flush |
| `sink.max-retries` | No | 3 | Integer | Maximum number of retry attempts on failure |
| `connection.max-retry-timeout` | No | 60s | Duration | Maximum timeout for retry attempts |

## Performance Tips

1. **Use COPY protocol** (`sink.use-copy-protocol = 'true'`) for bulk data loading - it's significantly faster than standard INSERT statements
2. **Tune buffer settings** - adjust `sink.buffer-flush.max-rows` and `sink.buffer-flush.interval` based on your throughput requirements
3. **Use upsert mode wisely** - only define PRIMARY KEY when you need update semantics, as it adds overhead
4. **Batch mode for analytics** - use batch mode (`EnvironmentSettings.inBatchMode()`) for offline ETL jobs
5. **Connection pooling** - the connector automatically manages connection pooling for optimal performance

## Documentation

The documentation of Apache Flink is located on the website: [https://flink.apache.org](https://flink.apache.org)
or in the `docs/` directory of the source code.

## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it.

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
