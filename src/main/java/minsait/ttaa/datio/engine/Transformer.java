package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;
import org.apache.spark.sql.functions.*;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df.printSchema();

        df = cleanData(df);
        df = exampleWindowFunction(df);
        df= Age_Range_Function(df);
        /*df=nationality_position(df);*/
        df = columnSelection(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);
        df.printSchema();

        // Uncomment when you want write your final output
        //write(df);
    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                long_name.column(),
                age.column(),
                age_range.column(),
                heightCm.column(),
                weight_kg.column(),
                nationality.column(),
                /*rank_by_nationality_position.column(),*/
                club_name.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                catHeightByPosition.column()

        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        Dataset<Row> df1 = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return df1;
    }

    /**
     * @param df2
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df2) {
        df2 = df2.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df2;
    }

    /**
     * @param df3 is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column  Age_Range_Function
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> exampleWindowFunction(Dataset<Row> df3) {
        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(50), "B")
                .otherwise("C");

        df3 = df3.withColumn(catHeightByPosition.getName(), rule);

        return df3;
    }
    /**
     * @param df4
     * @return   Age_Range_Function
     */
   private Dataset<Row> Age_Range_Function(Dataset<Row> df4) {

        /*df2 = df.withColumn(age_range.getName(), when(df.columns(age.column()) < 23,"A")
                .when(df.columns(age.column()) < 27,"B")
                .when(df.columns(age.column()) < 32,"C")
                .when(df.columns(age.column()) >= 32,"D")
                .otherwise("E"));

        df2.show();*/
        WindowSpec w = Window.partitionBy(age.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rango = when(rank.$less(23), "A")
                .when(rank.$less(27), "B")
                .when(rank.$less(32), "C")
                .otherwise("D");

        df4 = df4.withColumn(age_range.getName(), rango);


        return df4;
    }
 /* private Dataset<Row> nationality_position (Dataset<Row> df3) {


        df3.withColumn(rank_by_nationality_position.getName(),row_number().over(Window.partitionBy(nationality.column()).orderBy(overall .column().desc())));
        df3.show();
        return df3;
    }*/
 }