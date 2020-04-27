package com.sparklearning.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class AggregationExamples {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        //this line is not reading the dataset it just added transformation to execution plan
        Dataset<Row> dataset = sparkSession.read()
                                .option("header", true)
                                //.option("inferSchema",true) //takes one pss extra hence expensive operation to perform on big data
                                .csv("src/main/resources/exams/students.csv");

        //agg method
        Dataset<Row> maxSubjectResultByColumn = dataset.groupBy("subject")
                                                        .agg(max(col("score")).cast(DataTypes.IntegerType).alias("maxscore"),
                                                             min(col("score")).cast(DataTypes.IntegerType).alias("minscore"));
        maxSubjectResultByColumn.show();

    }
}
