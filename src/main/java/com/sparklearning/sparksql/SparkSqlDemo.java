package com.sparklearning.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlDemo {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/logs/biglog.txt");

        dataset.createOrReplaceTempView("logs");

        //Grouping, Aggregation and Ordering

        //query before optimization which will return dataset with monthnum column
        //Dataset<Row> levelMonthsDataset = sparkSession.sql("select level, date_format(datetime,'MMMM') as month,cast(first(date_format(datetime,'M')) as int) as monthnum, count(1) as total from logs group by level,month order by monthnum");

        //query after optimization which will return dataset without monthnum column and required result
        Dataset<Row> levelMonthsDataset = sparkSession.sql("select level, date_format(datetime,'MMMM') as month, count(1) as total from logs group by level,month order by cast(first(date_format(datetime,'M')) as int), level");

        //droping column using java API
        //levelMonthsDataset = levelMonthsDataset.drop("monthnum");

        levelMonthsDataset.show(100);

        sparkSession.close();
    }
}
