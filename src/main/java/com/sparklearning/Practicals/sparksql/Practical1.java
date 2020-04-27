package com.sparklearning.Practicals.sparksql;

import com.sparklearning.sparksql.SparkSqlApp;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;


public class Practical1 {

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


        dataset = dataset.groupBy("subject").pivot("year").agg(round(avg(col("score").cast(DataTypes.IntegerType)),2).alias("avg"),
                                                                    round(stddev(col("score").cast(DataTypes.IntegerType)),2).alias("stddev"));
        dataset.show(50);
        sparkSession.close();
    }
}
