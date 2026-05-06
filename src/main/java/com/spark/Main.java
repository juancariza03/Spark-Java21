package com.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

public class Main {
    private static final Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Java Demo")
                .master("local[*]")
                .getOrCreate();

        List<Person> people = List.of(
                new Person("Juan", 20, "IT"),
                new Person("Ana", 25, "HR"),
                new Person("Luis", 22, "IT"),
                new Person("Marta", 30, "Finance"),
                new Person("Sofia", 27, "IT"));

        Dataset<Row> df = spark.createDataFrame(people, Person.class);

        log.info("=== Schema ===");
        df.printSchema();

        log.info("=== Data ===");
        df.show();

        log.info("=== IT > 21 ===");
        df.filter(col("department").equalTo("IT"))
                .filter(col("age").gt(21))
                .show();

        log.info("=== Avg Age By Department ===");
        df.groupBy("department")
                .agg(avg("age").alias("avg_age"))
                .show();
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        spark.stop();
    }
}
