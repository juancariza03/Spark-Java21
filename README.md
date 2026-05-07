# ProyectoSpark

A minimal Apache Spark demo written in Java that loads the UCI _Adult_ dataset from a CSV file into a `Dataset<Row>` and runs basic schema inspection and aggregation through a dedicated `Analysis` helper.

## Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Dataset](#dataset)
- [How It Works](#how-it-works)
- [Run on Windows](#run-on-windows)
- [Expected Output](#expected-output)

## Overview

The application boots a local `SparkSession` (`local[*]`), reads `src/main/resources/adult.csv` with header inference and schema inference enabled, and demonstrates three operations:

1. Print the inferred schema.
2. Show the first rows of the dataset.
3. Count records grouped by the `income` column via `Analysis.incomes(dataset)`.

The program waits for a key press before stopping the Spark context, so the Spark UI at `http://localhost:4040` remains reachable while inspecting the run.

## Tech Stack

| Component                                     | Version                  |
| --------------------------------------------- | ------------------------ |
| Java                                          | 21                       |
| Apache Spark (Core + SQL + MLlib + Streaming) | 4.1.1                    |
| Scala binary                                  | 2.13                     |
| Log4j2                                        | 2.20.0                   |
| Gradle                                        | 9.4.1 (Wrapper included) |

## Project Structure

```
ProyectoSpark/
├── build.gradle
├── settings.gradle
├── gradlew / gradlew.bat
└── src/main/
    ├── java/com/spark/
    │   ├── Main.java        # Entry point: SparkSession + CSV load + orchestration
    │   └── Analysis.java    # Reusable Spark SQL transformations
    └── resources/
        ├── adult.csv         # UCI Adult dataset
        └── log4j2.properties # Silences Spark/Hadoop/Jetty logs
```

## Dataset

`adult.csv` is the well-known UCI _Adult_ (a.k.a. _Census Income_) dataset. Each row represents a person with demographic and employment attributes, and a binary `income` label (`<=50K` / `>50K`).

The columns are:

- age
- workclass
- fnlwgt
- education
- education-num
- marital-status
- occupation
- relationship
- race
- sex
- capital-gain
- capital-loss
- hours-per-week
- native-country
- income

Spark infers numeric columns (`age`, `fnlwgt`, `education-num`, `capital-gain`, `capital-loss`, `hours-per-week`) as integers and the rest as strings.

## How It Works

- `Main` builds the `SparkSession` in local mode and loads the CSV. The path constant `INPUT_CSV_FILE_PATH` points at `src/main/resources/adult.csv`. `Main` delegates aggregation to `Analysis.incomes(dataset)`, keeping I/O concerns separated from query logic.
- `Analysis.incomes(dataset)` returns a count of records per income bucket.
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
 |-- age: integer (nullable = true)
 |-- workclass: string (nullable = true)
 |-- fnlwgt: integer (nullable = true)
 |-- education: string (nullable = true)
 |-- education-num: integer (nullable = true)
 |-- marital-status: string (nullable = true)
 |-- occupation: string (nullable = true)
 |-- relationship: string (nullable = true)
 |-- race: string (nullable = true)
 |-- sex: string (nullable = true)
 |-- capital-gain: integer (nullable = true)
 |-- capital-loss: integer (nullable = true)
 |-- hours-per-week: integer (nullable = true)
 |-- native-country: string (nullable = true)
 |-- income: string (nullable = true)

=== Data ===
+---+----------------+------+------------+-------------+------------------+-----------------+--------------+------------------+------+------------+------------+--------------+--------------+------+
|age|       workclass|fnlwgt|   education|education-num|    marital-status|       occupation|  relationship|              race|   sex|capital-gain|capital-loss|hours-per-week|native-country|income|
+---+----------------+------+------------+-------------+------------------+-----------------+--------------+------------------+------+------------+------------+--------------+--------------+------+
| 47|    Self-emp-inc| 79627| Prof-school|           15|          Divorced|   Prof-specialty| Not-in-family|             White|  Male|       27828|           0|            50| United-States|  >50K|
| 55|         Private|151474|   Bachelors|           13|     Never-married|     Tech-support|Other-relative|             White|Female|           0|        1590|            38| United-States| <=50K|
| 26|         Private|132661|     HS-grad|            9|Married-civ-spouse|  Exec-managerial|          Wife|             White|Female|        5013|           0|            40| United-States| <=50K|
| 28|         Private|161674|     HS-grad|            9|     Never-married|Machine-op-inspct|     Unmarried|             White|Female|           0|           0|            40| United-States| <=50K|
| 36|         Private| 62346|     HS-grad|            9|Married-civ-spouse|     Craft-repair|       Husband|             Black|  Male|           0|           0|            40| United-States| <=50K|
| 40|         Private|227236|     HS-grad|            9|Married-civ-spouse|            Sales|       Husband|             White|  Male|           0|           0|            50| United-States| <=50K|
| 19|         Private|283033|        11th|            7|     Never-married|    Other-service| Not-in-family|             White|  Male|           0|           0|            40| United-States| <=50K|
| 63|Self-emp-not-inc|298249|   Bachelors|           13|Married-civ-spouse|            Sales|       Husband|             White|  Male|       10605|           0|            40| United-States|  >50K|
| 42|         Private|251229|   Bachelors|           13|Married-civ-spouse| Transport-moving|       Husband|             White|  Male|           0|           0|            40| United-States| <=50K|
| 76|         Private|199949|         9th|            5|Married-civ-spouse|  Protective-serv|       Husband|             White|  Male|           0|           0|            13| United-States| <=50K|
| 23|       State-gov|305498|   Assoc-voc|           11|     Never-married|     Tech-support| Not-in-family|             White|Female|           0|           0|            40| United-States| <=50K|
| 44|       Local-gov|202872|Some-college|           10|          Divorced|   Prof-specialty|     Unmarried|             White|Female|           0|           0|            25| United-States| <=50K|
| 27|         Private|198813|     HS-grad|            9|          Divorced|     Adm-clerical|     Own-child|             Black|Female|           0|           0|            40| United-States| <=50K|
| 33|     Federal-gov|129707|Some-college|           10|     Never-married|  Exec-managerial| Not-in-family|             White|  Male|           0|           0|            50| United-States| <=50K|
| 22|         Private|445758|     5th-6th|            3|     Never-married|Handlers-cleaners| Not-in-family|             White|  Male|           0|           0|            40|        Mexico| <=50K|
+---+----------------+------+------------+-------------+------------------+-----------------+--------------+------------------+------+------------+------------+--------------+--------------+------+
only showing top 20 rows

=== Incomes ===
+------+-----+
|income|count|
+------+-----+
| <=50K|37155|
|  >50K|11687|
+------+-----+
```
