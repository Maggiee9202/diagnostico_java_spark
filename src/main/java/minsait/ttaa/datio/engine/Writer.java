package minsait.ttaa.datio.engine;

import minsait.ttaa.datio.common.naming.PlayerOutput;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static org.apache.spark.sql.SaveMode.Overwrite;

abstract class Writer {

    static void write(Dataset<Row> df) {
        df
                .coalesce(2)
                .write()
                .partitionBy(teamPosition.getName())
                .mode(Overwrite)
                .parquet(OUTPUT_PATH);
    }
    static void write2(Dataset<Row> df1) {
        df1
                .coalesce(2)
                .write()
                .partitionBy(age.getName())
                .mode(Overwrite)
                .parquet(OUTPUT_PATH);
    }
   static void write3(Dataset<Row> df3) {
        df3
                .coalesce(2)
                .write()
                .partitionBy(teamPosition.getName())
                .mode(Overwrite)
                .parquet(OUTPUT_PATH);
    }

    }