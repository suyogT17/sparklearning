package com.sparklearning.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class LogsExampleUsingUDF {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/logs/biglog.txt");

        SimpleDateFormat input = new SimpleDateFormat("MMMM");
        SimpleDateFormat output = new SimpleDateFormat("M");


        sparkSession.udf().register("monthNum",(String rawMonth)->{
            Date date = input.parse(rawMonth);
            return Integer.parseInt(output.format(date));
        },DataTypes.IntegerType);

        dataset.createOrReplaceTempView("logs");
        Dataset<Row> levelMonthsDataset = sparkSession.sql("select level, date_format(datetime,'MMMM') as month, count(1) as total from logs group by level,month order by monthNum(month), level");

        levelMonthsDataset.show(100);



        sparkSession.close();
    }

}
