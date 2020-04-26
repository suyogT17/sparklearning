package com.sparklearning.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class SparkSqlBasics {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir","C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        Dataset<Row> dataset = sparkSession.read().option("header",true).csv("src/main/resources/exams/students.csv");
        dataset.show();   //shows first 20 rows in the dataset

        long numberOfRows = dataset.count();    //counts the number of rows in dataset
        System.out.println("Number of Rows: "+numberOfRows);

        /*
        * Accessing single row
        *
        * */
        Row firstRow = dataset.first();
        /*
        * Method return type is object because if we are using sparkSql with databases then it will have
        * different type of data such as int,dates.
        *
        * get(int columnIndex)= returns (type Object) the first element in the 3rd column
        * (2 is the index of the column which starts from 0)
        * */
        String subject = firstRow.get(2).toString();
        System.out.println("subject : "+subject);

        /*
        * getAs(String columnName)= return (type Object) if we have headers then we can use this method to
        * get elements of the specified column
        *
        * getAs() method try to perform automatic conversion with normal get method does not do.
        * */
        subject = firstRow.getAs("subject").toString();
        System.out.println("subject : "+subject);

        /* trying to fetch year but as the dataset is csv and the values are string so automatic conversion
        *  fails with RunTime Exception(ClassCastException)
        * */
        int year = Integer.parseInt(firstRow.getAs("year"));
        System.out.println("year: "+year);




        sparkSession.close();
    }

}
