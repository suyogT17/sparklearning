package com.sparklearning.Practicals.sparkml;

import com.sparklearning.sparksql.SparkSqlApp;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class CourseRecommendations {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        Dataset<Row> df = sparkSession.read().option("header", true).option("inferSchema", true).format("csv").load("src/main/resources/datasets/VPPcourseViews.csv");
/*
        Dataset<Row>[] splitDf = df.randomSplit(new double[]{.8,.2});
        Dataset<Row> trainingAndTestDf = splitDf[0];   //train data
        Dataset<Row> holdOutDf = splitDf[1];    //holdout data
*/

        ALS als = new ALS()
                .setMaxIter(10)
                .setRegParam(0.1)
                .setUserCol("userId")
                .setItemCol("courseId")
                .setRatingCol("proportionWatched");
        ALSModel alsModel = als.fit(df);
        alsModel.setColdStartStrategy("drop");//drop records for which prediction is NAN
        //Dataset<Row> predictions = alsModel.transform(holdOutDf);
        //predictions.show();

        Dataset<Row> userReccomendations = alsModel.recommendForAllUsers(5);
        userReccomendations.show();

        List<Row> userRecs = userReccomendations.takeAsList(5);
        for(Row row: userRecs){
            int userid = row.getAs(0);
            String reccom = row.getAs(1).toString();
            System.out.println("User "+ userid+ " we want to recommend "+reccom);
            System.out.println("This user has already watched ");
            df.filter("userid = "+userid).show();
        }

        //df.show();

    }

}
