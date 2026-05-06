# ProyectoSpark

A minimal Apache Spark demo written in Java that builds an in-memory `Dataset<Row>` from a list of `Person` objects and runs basic schema inspection, filtering, and aggregation operations.

## Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [How It Works](#how-it-works)
- [Run on Windows](#run-on-windows)
- [Expected Output](#expected-output)

## Overview

The application boots a local `SparkSession` (`local[*]`), creates a DataFrame from a hardcoded list of people, and demonstrates three operations:

1. Print the inferred schema.
2. Filter records where `department = "IT"` and `age > 21`.
3. Compute the average age grouped by `department`.

The program waits for a key press before stopping the Spark context, so the Spark UI at `http://localhost:4040` remains reachable while inspecting the run.

## Tech Stack

| Component                 | Version          |
| ------------------------- | ---------------- |
| Java                      | 21               |
| Apache Spark (Core + SQL) | 4.1.1            |
| Scala binary              | 2.13             |
| Log4j2                    | 2.20.0           |
| Gradle                    | Wrapper included |

## Project Structure

```
ProyectoSpark/
├── build.gradle
├── settings.gradle
├── gradlew / gradlew.bat
└── src/main/
    ├── java/com/spark/
    │   ├── Main.java        # Entry point: SparkSession + DataFrame ops
    │   └── Person.java      # Java record + explicit JavaBean getters for Spark's bean encoder
    └── resources/
        └── log4j2.properties # Silences Spark/Hadoop/Jetty logs
```

## How It Works

- `Person` is a Java `record` with components `name`, `age`, and `department`. It also declares explicit `getName()`, `getAge()` and `getDepartment()` methods. Spark's bean introspector only recognises JavaBean-style accessors (`getX` or `isX`), and the record's canonical accessors (`name()`, `age()`, `department()`) do not match that convention. Without those getters, `createDataFrame(people, Person.class)` produces an empty schema and column references like `col("department")` fail to resolve.
- `Main` builds the `SparkSession` in local mode, creates the DataFrame with `spark.createDataFrame(people, Person.class)`, and chains Spark SQL functions (`col`, `avg`) for the queries.
- `log4j2.properties` sets the root level to `OFF` so only the application's own `log.info` headers and Spark's `show()` tables are printed.

## Run on Windows

From the project root, execute:

```powershell
./gradlew run -q
```

The `-q` flag suppresses Gradle's own progress output, leaving only the application's tables on the console. Press `Enter` in the terminal when you want the program to stop the Spark context and exit.

## Expected Output

```
=== Schema ===
root
 |-- age: integer (nullable = false)
 |-- department: string (nullable = true)
 |-- name: string (nullable = true)

=== Data ===
+---+----------+-----+
|age|department| name|
+---+----------+-----+
| 20|        IT| Juan|
| 25|        HR|  Ana|
| 22|        IT| Luis|
| 30|   Finance|Marta|
| 27|        IT|Sofia|
+---+----------+-----+

=== IT > 21 ===
+---+----------+-----+
|age|department| name|
+---+----------+-----+
| 22|        IT| Luis|
| 27|        IT|Sofia|
+---+----------+-----+

=== Avg Age By Department ===
+----------+-------+
|department|avg_age|
+----------+-------+
|        HR|   25.0|
|   Finance|   30.0|
|        IT|   23.0|
+----------+-------+
```
