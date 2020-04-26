package com.sparklearning.sparksql;

import org.apache.spark.sql.SparkSession;

public class SparkSqlApp {

    private static SparkSession sparkSession;

    private SparkSqlApp(){

        sparkSession = SparkSession.builder().appName("learningSpark").master("local[*]")
                                            .config("spark.sql.warehouse.dir","file:///D:/Suyog Work/Study/Spark/tmp") //only for windows
                                            .getOrCreate();
    }

    public static SparkSession getSparkSession(){

        if(sparkSession == null){
            SparkSqlApp sparkSqlApp = new SparkSqlApp();
        }
        return sparkSession;
    }

}
