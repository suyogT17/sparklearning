package com.sparklearning.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;


public class UserDefinedFunctionDemo {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        //this line is not reading the dataset it just added transformation to execution plan
        Dataset<Row> dataset = sparkSession.read()
                .option("header", true)
                //.option("inferSchema",true) //takes one pass extra hence expensive operation to perform on big data
                .csv("src/main/resources/exams/students.csv");

        /*
        * withColumn() allows us to add new column to the dataframe
        * lit() - it stands for literal
        * */
        Dataset<Row> datasetWithAddedColumn = dataset.withColumn("pass",lit(col("grade").equalTo("A+")));

        sparkSession.udf().register("hasPassed",(String grade,String subject) -> {
            if(grade.startsWith("A")|| (grade.startsWith("B")||grade.startsWith("C") && !subject.equals("Biology")))
                return  "pass";
            return "fail";
            }, DataTypes.StringType);

        dataset = dataset.withColumn("pass",callUDF("hasPassed",col("grade"),col("subject")));
        dataset.show();
        sparkSession.close();

    }

}
