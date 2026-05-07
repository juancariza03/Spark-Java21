package com.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Analysis {
    public static Dataset<Row> incomes(Dataset<Row> dataset) {
        return dataset.groupBy("income").count();
    }
}
