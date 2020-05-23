package com.sparklearning.Practicals.sparkml;

import com.sparklearning.sparksql.SparkSqlApp;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitorsClustering {

    public static void main(String[] args) {


        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        Dataset<Row> df = sparkSession.read().option("header", true).option("inferSchema", true).format("csv").load("src/main/resources/datasets/GymCompetition.csv");

       // df = df.withColumnRenamed("NoOfReps", "label");


        //df = df.select("label", "Gender", "Age", "Height", "Weight");


        StringIndexer indexer = new StringIndexer().setInputCol("Gender").setOutputCol("Gen");
        df = indexer.fit(df).transform(df);

        OneHotEncoderEstimator oneHotEncoderEstimator = new OneHotEncoderEstimator()
                .setInputCols(new String[]{"Gen"})
                .setOutputCols(new String[]{"GenderVector"});

        df = oneHotEncoderEstimator.fit(df).transform(df);

        String []features = {"GenderVector","Age","Height","Weight","NoOfReps"};

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(features)
                .setOutputCol("features");

        df = assembler.transform(df);

        KMeans kMeans = new KMeans();

        for(int numClusters=2;numClusters<=8;numClusters++) {
            System.out.println("Number Of clusters: "+numClusters);
            kMeans.setK(numClusters);

            KMeansModel model = kMeans.fit(df);
            Dataset<Row> prediction = model.transform(df);
            //      prediction.show();

/*        Vector[] clusterCentersmodel = model.clusterCenters();
        for (Vector v : clusterCentersmodel){
            System.out.println(v);
        }*/

            prediction.groupBy("prediction").count().show();

            System.out.println("Sum Squared Error " + model.computeCost(df));
            ClusteringEvaluator evaluator = new ClusteringEvaluator();
            System.out.println("Slihouette with squared euclidean distance is " + evaluator.evaluate(prediction));
            //df.show();
        }
        }
}
