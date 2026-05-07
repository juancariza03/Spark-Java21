package com.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
    private static final Logger log = LogManager.getLogger(Main.class);
    public static final String INPUT_CSV_FILE_PATH = "src/main/resources/adult.csv";

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Advanced Exploration")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(INPUT_CSV_FILE_PATH);

        log.info("=== Schema ===");
        dataset.printSchema();

        log.info("=== Data ===");
        dataset.show();

        log.info("=== Incomes ===");
        Dataset<Row> income = Analysis.incomes(dataset);
        income.show();

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        spark.stop();
    }
}
