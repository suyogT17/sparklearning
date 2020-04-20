package com.sparklearning.demo;

public class SortingAndCoalesceDemo {

        public static void main(String[] args) {


        /*
        * Refer Practical/Practical1.java
        *
        * if use foreach() method to print the elements of RDD after sorting will not
        * give us the required results as data is divided over number of partitions.
        *
        * coalesce(n) method allow us to specify number of partition (n =>  number of partition)
        * after apply this coalesce(1) spark will collect all data into single partition.
        * and we will get the required result
        *
        * But using coalesce(1) is not a proper solution for sorting data as it creates new problem
        * by combining all the data into single partition using coalesce(1) will keep the data into single node
        * i.e on the single physical machine so there are chances that we can go out of memory while further processing
        * the data and we will not able to take advantage of spark anymore
        *
        * Number of partitions are depend on the size of input
        * if we are working with HDFS then size of partition = size of HDFS block (64MB)
        *
        * we dont need to really know about number of partitions in the spark but need to know about the shuffles for performance
        * you should get correct sorting regardless of how spark has organised the data into partitions
        *
        * foreach() working:
        * function provided in the foreach() will be applied to each partition.
        * so as we are using spark in local there are cores and for each core there are thread
        * so java use threads to execute the function on each partition in parallel
        * foreach() method's output depends on the execution of thread
        *
        * */


        }



}
