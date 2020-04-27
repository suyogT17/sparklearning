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

        sparkSession.conf().set("spark.sql.shuffle.partitions","10");

        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/logs/biglog.txt");

        dataset.createOrReplaceTempView("logs");

        //Grouping, Aggregation and Ordering

        //query before optimization which will return dataset with monthnum column
        //unoptimized Query
        //Dataset<Row> levelMonthsDataset = sparkSession.sql("select level, date_format(datetime,'MMMM') as month,cast(first(date_format(datetime,'M')) as int) as monthnum, count(1) as total from logs group by level,month order by monthnum");

        //optimized Query
        // Dataset<Row> levelMonthsDataset = sparkSession.sql("select level, date_format(datetime,'MMMM') as month, count(1) as total,first(cast((date_format(datetime,'M')) as int)) as monthnum from logs group by level,month order by monthnum, level");

        //query after optimization which will return dataset without monthnum column and required result
        //below query is unoptimized so it will use sortAggregation
        //Dataset<Row> levelMonthsDataset = sparkSession.sql("select level, date_format(datetime,'MMMM') as month, count(1) as total from logs group by level,month order by cast(first(date_format(datetime,'M')) as int), level");

        //optimized Query that will use hashAggregation
        Dataset<Row> levelMonthsDataset = sparkSession.sql("select level, date_format(datetime,'MMMM') as month, count(1) as total from logs group by level,month order by first(cast(date_format(datetime,'M') as int)), level");


        //droping column using java API
        //levelMonthsDataset = levelMonthsDataset.drop("monthnum");

        levelMonthsDataset.show(100);
        levelMonthsDataset.explain();
        sparkSession.close();
    }
}
