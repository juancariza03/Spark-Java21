# ProyectoSpark

A Java demo of **Apache Spark Structured Streaming** that simulates a continuous CSV feed: it splits the UCI _Adult_ dataset into chunks, drips them into a watched directory on a schedule, and a Spark streaming query persists every micro-batch as Parquet. When the user presses `Enter`, the same job re-reads the Parquet output in batch mode and runs an aggregation through a dedicated `Analysis` helper.

## Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Dataset](#dataset)
- [How It Works](#how-it-works)
- [Runtime Layout](#runtime-layout)
- [Configuration Constants](#configuration-constants)
- [Run on Windows](#run-on-windows)
- [Expected Output](#expected-output)

## Overview

The application boots a local `SparkSession` (`local[*]`) and orchestrates two phases:

1. **Streaming phase.** A scheduled feeder moves pre-split CSV chunks into `build/stream/input/` one at a time. A `readStream` query picks them up and writes them to `build/stream/output/` as Parquet, with checkpointing in `build/stream/checkpoint/`.
2. **Batch phase.** When the user presses `Enter`, the job reads the Parquet output back, logs its row count, prints the schema and a sample, and delegates aggregation to `Analysis.incomes(dataset)` to count records per `income` bucket.

The program then waits for a second key press before stopping the Spark context, so the Spark UI at `http://localhost:4040` remains reachable while inspecting the run.

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
    │   ├── Main.java        # Entry point: streaming feeder + readStream/writeStream + batch re-read
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

1. **`resetStreamDirs()`** recursively clears `build/stream/` (if any) and recreates the four working directories: `staged/`, `input/`, `output/`, `checkpoint/`. This guarantees a clean run every time.
2. **`splitCsv(source, stagingDir, CHUNK_SIZE)`** reads `adult.csv` once, preserves its header on every chunk, and writes contiguous slices of `CHUNK_SIZE` rows into `staged/chunk-000.csv`, `chunk-001.csv`, …
3. **`spark.readStream()`** opens a streaming `Dataset<Row>` over `input/`, applying `adultSchema()` and `maxFilesPerTrigger=1` so each micro-batch consumes exactly one chunk.
4. **`writeStream()`** sinks the stream to Parquet under `output/`, using `checkpoint/` for offsets and append output mode.
5. **`startChunkFeeder(chunks)`** schedules a single-threaded `ScheduledExecutorService` (daemon thread `chunk-feeder`) that moves one staged chunk into `input/` every `FEED_INTERVAL_SECONDS` seconds (after a 1-second initial delay), simulating a slow producer.
6. **User-gated batch read**, when the user presses `Enter`, `spark.read().parquet(output/)` re-reads everything that the stream has persisted so far, logs the row count, prints the schema and a sample, and finally calls `Analysis.incomes(batchResult).show()`.
7. **Shutdown** — a second `System.in.read()` keeps Spark alive (and the UI reachable) until the user presses `Enter` again, at which point `spark.stop()` runs.

## Runtime Layout

While the application runs, it creates and uses the following directory tree (relative to the project root):

```
build/stream/
├── staged/        # CSV chunks waiting to be fed
├── input/         # Watched by readStream — one file per micro-batch
├── output/        # Parquet sink written by writeStream
└── checkpoint/    # Structured Streaming checkpoint state
```

This whole tree is wiped and recreated at the start of each run.

## Configuration Constants

Defined at the top of `Main.java` and easy to tweak:

| Constant                | Default                        | Purpose                                   |
| ----------------------- | ------------------------------ | ----------------------------------------- |
| `SOURCE_CSV`            | `src/main/resources/adult.csv` | Source file to be split into chunks       |
| `STREAM_BASE`           | `build/stream`                 | Root of all runtime streaming directories |
| `CHUNK_SIZE`            | `5000`                         | Rows per generated chunk                  |
| `FEED_INTERVAL_SECONDS` | `3`                            | Delay between chunk drops into `input/`   |

## Run on Windows

From the project root, execute:

```powershell
./gradlew run -q
```

The `-q` flag suppresses Gradle's own progress output, leaving only the application's tables on the console. Press `Enter` once when you want to trigger the batch re-read of the Parquet output, and a second time to stop the Spark context and exit.

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
| <=50K| ... |
|  >50K| ... |
+------+-----+
```
