package com.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
    private static final Logger log = LogManager.getLogger(Main.class);

    private static final Path SOURCE_CSV = Paths.get("src/main/resources/adult.csv");
    private static final Path STREAM_BASE = Paths.get("build/stream");
    private static final Path STAGED_DIR = STREAM_BASE.resolve("staged");
    private static final Path INPUT_DIR = STREAM_BASE.resolve("input");
    private static final Path OUTPUT_DIR = STREAM_BASE.resolve("output");
    private static final Path CHECKPOINT_DIR = STREAM_BASE.resolve("checkpoint");

    private static final int CHUNK_SIZE = 5000;
    private static final long FEED_INTERVAL_SECONDS = 3;

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Structured Streaming")
                .master("local[*]")
                .getOrCreate();

        resetStreamDirs();
        List<Path> chunks = splitCsv(SOURCE_CSV, STAGED_DIR, CHUNK_SIZE);

        Dataset<Row> stream = spark.readStream()
                .option("header", "true")
                .schema(adultSchema())
                .option("maxFilesPerTrigger", 1)
                .csv(INPUT_DIR.toAbsolutePath().toString());

        stream.writeStream()
                .format("parquet")
                .option("path", OUTPUT_DIR.toAbsolutePath().toString())
                .option("checkpointLocation", CHECKPOINT_DIR.toAbsolutePath().toString())
                .outputMode("append")
                .start();

        startChunkFeeder(chunks);

        log.info("Press Enter to read the parquet");
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Dataset<Row> batchResult = spark.read().parquet(OUTPUT_DIR.toAbsolutePath().toString());

        log.info("=== Schema ===");
        batchResult.printSchema();

        log.info("=== Data ===");
        batchResult.show();

        log.info("=== Incomes ===");
        Analysis.incomes(batchResult).show();

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        spark.stop();
    }

    private static ScheduledExecutorService startChunkFeeder(List<Path> chunks) {
        ScheduledExecutorService feeder = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "chunk-feeder");
            t.setDaemon(true);
            return t;
        });
        Iterator<Path> iter = chunks.iterator();
        feeder.scheduleAtFixedRate(() -> {
            if (!iter.hasNext())
                return;
            Path chunk = iter.next();
            try {
                Files.move(chunk, INPUT_DIR.resolve(chunk.getFileName()));
                log.info("Feed -> {}", chunk.getFileName());
            } catch (IOException e) {
                log.error("Error moving chunk {}", chunk, e);
            }
        }, 1, FEED_INTERVAL_SECONDS, TimeUnit.SECONDS);
        return feeder;
    }

    private static StructType adultSchema() {
        return new StructType()
                .add("age", DataTypes.IntegerType)
                .add("workclass", DataTypes.StringType)
                .add("fnlwgt", DataTypes.IntegerType)
                .add("education", DataTypes.StringType)
                .add("education-num", DataTypes.IntegerType)
                .add("marital-status", DataTypes.StringType)
                .add("occupation", DataTypes.StringType)
                .add("relationship", DataTypes.StringType)
                .add("race", DataTypes.StringType)
                .add("sex", DataTypes.StringType)
                .add("capital-gain", DataTypes.IntegerType)
                .add("capital-loss", DataTypes.IntegerType)
                .add("hours-per-week", DataTypes.IntegerType)
                .add("native-country", DataTypes.StringType)
                .add("income", DataTypes.StringType);
    }

    private static void resetStreamDirs() throws IOException {
        if (Files.exists(STREAM_BASE)) {
            try (Stream<Path> paths = Files.walk(STREAM_BASE)) {
                paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException ignored) {
                    }
                });
            }
        }
        Files.createDirectories(STAGED_DIR);
        Files.createDirectories(INPUT_DIR);
        Files.createDirectories(OUTPUT_DIR);
        Files.createDirectories(CHECKPOINT_DIR);
    }

    private static List<Path> splitCsv(Path source, Path stagingDir, int rowsPerChunk) throws IOException {
        List<Path> chunks = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(source)) {
            String header = reader.readLine();
            if (header == null)
                return chunks;

            int chunkIdx = 0;
            int linesInChunk = 0;
            BufferedWriter writer = null;
            String line;
            while ((line = reader.readLine()) != null) {
                if (writer == null) {
                    Path chunk = stagingDir.resolve(String.format("chunk-%03d.csv", chunkIdx++));
                    writer = Files.newBufferedWriter(chunk);
                    writer.write(header);
                    writer.newLine();
                    chunks.add(chunk);
                    linesInChunk = 0;
                }
                writer.write(line);
                writer.newLine();
                if (++linesInChunk >= rowsPerChunk) {
                    writer.close();
                    writer = null;
                }
            }
            if (writer != null)
                writer.close();
        }
        return chunks;
    }
}
