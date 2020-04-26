package com.sparklearning.rddprogramming;

import com.sparklearning.model.IntegerWithSquareRoot;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;

public class RDDMapReduceDemo {

    public static void main(String[] args) {

        Logger  logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        App app = new App();
        /*
        * this object represents the connection with our spark cluster
        * allow us to communicate with spark
        * */
        JavaSparkContext sc = app.getSparkContext();

        List<Integer> inputData = new ArrayList<>();
        inputData.add(12);
        inputData.add(2);
        inputData.add(33);
        inputData.add(199);

        //loads data into rdd
        JavaRDD<Integer>  intRDD = sc.parallelize(inputData);

        //Reduce Operation - summing data in RDD
        Integer result = intRDD.reduce((value1,value2) -> (value1+value2));
        System.out.println("\n"+result);

        /*Map operation - allows us to transform the structure of RDD from one form to another
        * finding square root of number
        * RDDs are mutable so spark returns new RDD after we modified the current RDD
        * Type of input data can be different than the type of output data
        */
        JavaRDD<Double> sqrtRDD = intRDD.map((value) -> Math.sqrt(value));
        sqrtRDD.foreach(value-> System.out.println(value));

        //how many elements in sqrtRDD using count() function
        Long count = sqrtRDD.count();
        System.out.println(count);

        //count using map and reduce
        JavaRDD<Long> mapRDD = sqrtRDD.map(value-> 1L);
        Long countWithMapReduce = mapRDD.reduce((value1,value2) -> value1+value2);
        System.out.println(countWithMapReduce);

        //tuple using java
        JavaRDD<IntegerWithSquareRoot> numSquarrtRDD = intRDD.map(value -> new IntegerWithSquareRoot(value));

        /*
        * creating tuple in scala
        * val mytuple = (9,3) this will compile and internally converted to
        * val mytuple = new Tuple2(9,3) where  2 represents number of arguments
        * */
        JavaRDD<Tuple2<Integer,Double>> numSqrtTupleRDD = intRDD.map(value ->  new Tuple2(value, Math.sqrt(value)));
        numSqrtTupleRDD.foreach(value -> System.out.println(value));

        sc.close();
    }
}
