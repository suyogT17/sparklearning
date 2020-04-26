package com.sparklearning.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


public class FiltersDemo {


    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        //this line is not reading the dataset it just added to execution plan
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");

        /*
        * Filters using String Expression:
        *   we need to specify condition in the same format as we specify in database WHERE clause.
        * */
        Dataset<Row> datasetModernArt = dataset.filter("subject = 'Modern Art' AND year >= 2007");
        //datasetModernArt.show();

        /*
        * Filters using lambda:
        *   we need to specify condition in the lambda expression
        * */

        datasetModernArt = dataset.filter(value -> value.getAs("subject").equals("Modern Art") && new Integer(value.getAs("year"))==2007);
        //datasetModernArt.show();

        /*
        * Filters using columns:
        *   more programmatic approach
        *   Makes use of Column class
        * */

        Column subjectColumn = dataset.col("subject");
        Column yearColumn = dataset.col("year");
        datasetModernArt  = dataset.filter(subjectColumn.equalTo("Modern Art").and(yearColumn.geq(2007)));
        //datasetModernArt.show();

        /*
        * Alternate way to do above thing by using function class
        * */
        datasetModernArt  = dataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007)));
        datasetModernArt.show();


        sparkSession.close();
    }
}
