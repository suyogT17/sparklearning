package com.sparklearning.demo;

import com.sparklearning.App;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class ReadFileDemo {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir","C:\\hadoop");

        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);
        App app = new App();
        JavaSparkContext sc = app.getSparkContext();

        /*
        * sc.textFile() with throw out of memory exception as we are working on local machine if the file is too big
        * we can load data from distributed file system (HDFS,Amazon S3 ...)
        *
        * */
        //returns exception but does not cause program to stop
        JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");


        initialRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .foreach(value -> System.out.println(value));
    }

}
