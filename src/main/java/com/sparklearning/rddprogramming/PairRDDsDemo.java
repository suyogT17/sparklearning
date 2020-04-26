package com.sparklearning.rddprogramming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Iterables;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PairRDDsDemo {

    public static void main(String[] args) {

        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);
        App app = new App();
        JavaSparkContext sc = app.getSparkContext();
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        JavaRDD<String> originalLogs = sc.parallelize(inputData);
        /*
         * PairRDD  is like a map containing key value but it can have duplicate keys
         * provides extra methods to perform operation with keys
         * mapToPair() method use to convert RDD to PairRDD
         * Tuple2 is used to provide key,value pair to PairRDDs
         *
         * GroupByKey can lead to severe performance problems so use it unless there is no another alternative
         * causes crashes on cluster
         * */

        //splitting into key=log Level and value=Date
        JavaPairRDD<String,String> splitLogs1 = originalLogs.mapToPair(value -> new Tuple2<>(value.split(":")[0],value.split(":")[1]));

        //splitting into key=log Level and value=1
        JavaPairRDD<String,Long> splitLogs2 = originalLogs.mapToPair(value -> new Tuple2<>(value.split(":")[0],1L));

        //counting sub based on key
        JavaPairRDD<String,Long> ansRDD = splitLogs2.reduceByKey((value1,value2) -> value1+value2);
        //ansRDD.foreach(value -> System.out.println(value._1+" "+value._2));  //accessing values of tuple

        /*
        * Using GroupBy key
        */
        //printing size of iterable return by groupBy using spliterator
        splitLogs2.groupByKey().foreach(value -> System.out.println(value._1+" "+value._2.spliterator().getExactSizeIfKnown() ));

        System.out.println("---------------------------------------------");

        //printing size of iterable return by groupBy using guava
        splitLogs2.groupByKey().foreach(value -> System.out.println(value._1+" "+ Iterables.size(value._2)));
        sc.close();
    }

}
