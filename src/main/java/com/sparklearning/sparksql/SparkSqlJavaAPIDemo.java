package com.sparklearning.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class SparkSqlJavaAPIDemo {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/logs/biglog.txt");

        /*
        * select()= allows to specify column name as argument
        * selectExpr() = allows us to specify expression as argument
        * */

        /*
            dataset = dataset.selectExpr("level","date_format(datetime,'MMMM') as month");
            dataset.show();
        */
        //alternate way

        dataset = dataset.select(col("level"),
                        date_format(col("datetime"),"MMMM").alias("month"),
                        date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));

        //Dataset<Row> levelMonthsDataset = sparkSession.sql("select level, date_format(datetime,'MMMM') as month, count(1) as total from logs group by level,month order by cast(first(date_format(datetime,'M')) as int), level");

        //groupBy does not return dataset object we need to use aggregation function
        //anything which is not part of grouping or aggregation gets discarded
        Dataset<Row> newDataset = dataset.groupBy(col("level"),col("month"),col("monthnum"))
                .count()
                .orderBy(col("monthnum"),col("level"))
                .drop(col("monthnum"));
        //newDataset.show(100);

        //Pivot Table
        String month[] = {"January","February","March","April","May","June","July","August","September","October","Nov","November","December"};
        List<Object> monthList = Arrays.asList(month);
        dataset = dataset.groupBy("level").pivot("month",monthList).count().na().fill(0);
        dataset.show();

        sparkSession.close();
    }

}
