package com.sparklearning.demo;

import com.sparklearning.App;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class RDDJoinsDemo {

    public static void main(String[] args) {
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        App app = new App();
        JavaSparkContext sc = app.getSparkContext();

        List<Tuple2<Integer,Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(2,18));
        visitsRaw.add(new Tuple2<>(6,4));
        visitsRaw.add(new Tuple2<>(10,9));

        List<Tuple2<Integer,String>> usersRaw =  new ArrayList<>();
        usersRaw.add(new Tuple2<>(1,"John"));
        usersRaw.add(new Tuple2<>(2,"Bob"));
        usersRaw.add(new Tuple2<>(3,"Alan"));
        usersRaw.add(new Tuple2<>(4,"Doris"));
        usersRaw.add(new Tuple2<>(5,"Marybelle"));
        usersRaw.add(new Tuple2<>(6,"Raquel"));

        JavaPairRDD<Integer,Integer> visitsRdd = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer,String> usersRDD = sc.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRDD = visitsRdd.join(usersRDD);
        /*
        * Optional is used to indicate that the value might be empty
        * */
        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftJoinedRDD = visitsRdd.leftOuterJoin(usersRDD);
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightJoinedRDD = visitsRdd.rightOuterJoin(usersRDD);
        JavaPairRDD<Integer,Tuple2<Optional<Integer>,Optional<String>>> outerJoinedRDD = visitsRdd.fullOuterJoin(usersRDD);
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesianProductRDD = visitsRdd.cartesian(usersRDD);


        System.out.println("-------------inner join-------------");
        joinedRDD.foreach(value -> System.out.println(value));

        System.out.println("-------------left outer join-------------");
        leftJoinedRDD.foreach(value -> System.out.println(value));

        System.out.println("------------fetching data----------------");
        /*
         * get()
         * isPresent()
         * orElse(String) will return string specified as argument if the optional is empty
         * */
        leftJoinedRDD.foreach(value -> System.out.println(value._2._2.orElse("empty").toUpperCase()));


        System.out.println("-------------right outer join-------------");
        rightJoinedRDD.foreach(value -> System.out.println(value));

        System.out.println("-------------printing result-------------");
        rightJoinedRDD.foreach(value -> System.out.println("user "+value._2._2+" has visited site "+value._2._1.orElse(0)+" times"));

        System.out.println("-------------full outer join-------------");
        outerJoinedRDD.foreach(value -> System.out.println(value));

        System.out.println("-------------cartision product-------------");
        cartesianProductRDD.foreach(value -> System.out.println(value));



        sc.close();
    }


}
