package com.sparklearning.demo;

import com.sparklearning.App;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMapsFiltersDemo {

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

        JavaRDD<String> sentences = sc.parallelize(inputData);
        /*
        * FlatMaps:
        * it is a function which takes the input and return one or more values a output (iterator)
        * it create the RDD with single flat collection
        * flatmap needs to be return iterator
        * */

        //flatmap for splitting string
        JavaRDD<String> stringRDD = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
        stringRDD.foreach(value -> System.out.print(value+"\t"));

        /*
        * Filters:
        * use to filter daat in RDD
        * it will remove condition which iis false
        * */

        //filter for excluding elements which have length 1
        stringRDD.filter(value -> (value.length() > 1)).foreach(value -> System.out.print(value+"\t"));

    }

}
