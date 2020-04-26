package com.sparklearning.rddprogramming;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Hello world!
 *
 */
public class App 
{
    private SparkConf conf;
    private JavaSparkContext sc;

    public App(){
        /*
         * setting to local[*] means it will use all available cores on your machine as we are using local configurations
         * if we specify only local then app will run only on single thread
         * */
        conf = new SparkConf().setAppName("learningSpark").setMaster("local[*]");

        /*
         * this object represents the connection with our spark cluster
         * allow us to communicate with spark
         * */
        sc = new JavaSparkContext(conf);

    }

    public SparkConf getSparkConf(){
          return conf;
    }

    public JavaSparkContext getSparkContext(){
        return sc;
    }
}
