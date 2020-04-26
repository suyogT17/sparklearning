package com.sparklearning.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FullSqlSyntaxDemo {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");
        dataset.createOrReplaceTempView("student_table");
        Dataset<Row> studentTable = sparkSession.sql("select subject,min(score),avg(score),max(score) from student_table group by subject");
        studentTable.show();

        sparkSession.close();
    }
}
