package com.sparklearning.Practicals.rddprogramming;

import com.sparklearning.rddprogramming.App;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Practical2 {

    public  static boolean testMode = false;
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        App app = new App();
        JavaSparkContext sc = app.getSparkContext();

        testMode = false;

        //Input Data
        JavaPairRDD<Integer,Integer> chaptersRDD =  getChaptersRDD(sc);
        JavaPairRDD<Integer,String> titlesRDD = getTitlesRDD(sc);
        JavaPairRDD<Integer,Integer> viewsRDD = getViewsRDD(sc);

        //warmup
        JavaPairRDD<Integer,Long> courseChapFreqRdd = chaptersRDD.mapToPair(value -> new Tuple2<>(value._2,1L))
                .reduceByKey((value1,value2) -> value1+value2);

        //courseChapFreqRdd.take(3).forEach(value -> System.out.println(value));

        //Step 1 - remove duplicated views
        viewsRDD = viewsRDD.distinct();

        //viewsRDD.foreach(value -> System.out.println(value));

        //Step 2 - adding courseId to
        JavaPairRDD<Integer,Tuple2<Integer,Integer>> courseChapterUserRdd = chaptersRDD.join(viewsRDD.mapToPair(value -> new Tuple2<>(value._2,value
        ._1)));
        //courseChapterUserRdd.foreach(value -> System.out.println(value));

        //Step 3 - Drop chapterId to count unique views
        JavaPairRDD<Tuple2<Integer,Integer>,Long> userCourseFreq = courseChapterUserRdd.mapToPair(value -> new Tuple2<>(value._2,1L));
        //userCourseFreq.foreach(value -> System.out.println(value));

        //Step 4 - Counting view of each Chapter
        JavaPairRDD<Tuple2<Integer,Integer>,Long> chapterViewsCount = userCourseFreq.reduceByKey((value1,value2) -> value1+value2);
        //chapterViewsCount.foreach(value -> System.out.println(value));

        //Step 5 - drop userId
        JavaPairRDD<Integer,Long> chapterViews = chapterViewsCount.mapToPair(value-> new Tuple2<>(value._1._1,value._2));

        //Step 6- joining with courseChapFreqRdd
        JavaPairRDD<Integer, Tuple2<Long, Long>> chapterCompletionPercentage =  chapterViews.join(courseChapFreqRdd);
        //chapterCompletionPercentage.foreach(value -> System.out.println(value));

        //Step 7- assigning scores to chapters
        JavaPairRDD<Integer,Long> chapterScore = chapterCompletionPercentage.mapToPair(value -> new Tuple2<>(value._1,((value._2._1*100/value._2._2) < 25)? 0L:(((value._2._1*100/value._2._2)*100 > 25) && ((value._2._1*100/value._2._2) < 50))?2:(((value._2._1*100/value._2._2) > 50) && ((value._2._1*100/value._2._2) <90))?4:10));

        //chapterScore.foreach(value -> System.out.println(value));

        //Step 8- calculating total scores for chapters
        JavaPairRDD<Integer,Long> chapetersTotalScore = chapterScore.reduceByKey((value1,value2) -> value1+value2 );
        //chapetersTotalScore.foreach(value -> System.out.println(value));

        //Step 8 - Adding titles and making view counts as key
        JavaPairRDD<Long,Tuple2<Integer,String>> chaptersWithScoreAndTitle = chapetersTotalScore.join(titlesRDD).mapToPair(value -> new Tuple2<>(value._2._1,new Tuple2<>(value._1,value._2._2)));
        //chaptersWithScoreAndTitle.foreach(value -> System.out.println(value));

        //Step 9 - sorting based on chapter scores
        JavaPairRDD<Long,Tuple2<Integer,String>> sortedChaptersAndScoreAndTitle = chaptersWithScoreAndTitle.sortByKey(false);

        //Step 10- printing top 10 Results
        sortedChaptersAndScoreAndTitle.take(10).forEach(value -> System.out.println("Chap "+value._2._1+" - "+value._2._2+" -> Score - "+value._1));

        /*sortedChaptersAndScoreAndTitle
                .coalesce(1)
                .map(value -> "Chap "+value._2._1+" - "+value._2._2+" -> Score - "+value._1)
                .saveAsTextFile("src/main/resources/viewing figures/output");

*/
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        sc.close();
    }

    public static JavaPairRDD<Integer,Integer>  getChaptersRDD(JavaSparkContext sc) {

        if (testMode)
        {
            // (chapterId, (courseId, courseTitle))
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96,  1));
            rawChapterData.add(new Tuple2<>(97,  1));
            rawChapterData.add(new Tuple2<>(98,  1));
            rawChapterData.add(new Tuple2<>(99,  2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));
            return sc.parallelizePairs(rawChapterData);
        }

        return sc.textFile("src/main/resources/viewing figures/chapters.csv").mapToPair(value -> new Tuple2<>(new Integer(value.split(",")[0]),new Integer(value.split(",")[1])));
    }

    public static JavaPairRDD<Integer,String>  getTitlesRDD(JavaSparkContext sc) {
        if (testMode)
        {
            // (chapterId, title)
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
            rawTitles.add(new Tuple2<>(1, "How to find a better job"));
            rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs(rawTitles);
        }

       return sc.textFile("src/main/resources/viewing figures/titles.csv").mapToPair(value -> new Tuple2<>(new Integer(value.split(",")[0]),value.split(",")[1]));
    }

    public static JavaPairRDD<Integer,Integer> getViewsRDD(JavaSparkContext sc){

        if (testMode)
        {
            // Chapter views - (userId, chapterId)
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            return  sc.parallelizePairs(rawViewData);
        }

        return  sc.textFile("src/main/resources/viewing figures/views-*.csv").mapToPair(value -> new Tuple2<>(new Integer(value.split(",")[0]),new Integer(value.split(",")[1])));
    }


}




