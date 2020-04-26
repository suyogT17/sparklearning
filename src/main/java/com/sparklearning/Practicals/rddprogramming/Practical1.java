package com.sparklearning.Practicals.rddprogramming;


import com.sparklearning.rddprogramming.App;
import com.sparklearning.helper.Util;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Scanner;

public class Practical1 {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir","C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        App app =  new App();
        JavaSparkContext sc = app.getSparkContext();

        JavaRDD<String> inputRDD = sc.textFile("src/main/resources/subtitles/input.txt");
        inputRDD.map(value->value.replaceAll("[^a-zA-Z\\s]",""))
                .map(value -> value.toLowerCase())
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .filter(value -> !(value.trim().length()==0) )
                .filter(value -> Util.isNotBoring(value))
                .mapToPair(value -> new Tuple2<String,Long>(value,1L))
                .reduceByKey((value1,value2) -> value1 + value2)
                .mapToPair(value-> new Tuple2<Long,String>(value._2,value._1))
                .sortByKey(false)
                //.getNumPartitions(); //to print number of partition
                //.coalesce(1)
                //.foreach(value -> System.out.println(value));
                 .take(10)
                 .forEach(System.out::println);

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        sc.close();
    }

}
