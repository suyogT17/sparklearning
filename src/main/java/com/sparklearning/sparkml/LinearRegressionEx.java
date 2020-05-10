package com.sparklearning.sparkml;

import com.sparklearning.sparksql.SparkSqlApp;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LinearRegressionEx {

    public static void main(String[] args) {


        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        Dataset<Row> df = sparkSession.read().option("header",true).option("inferSchema",true).format("csv").load("src/main/resources/datasets/GymCompetition.csv");

        df = df.withColumnRenamed("NoOfReps","label");


        df = df.select("label","Gender","Age","Height","Weight");


        StringIndexer indexer = new StringIndexer().setInputCol("Gender").setOutputCol("Gen");
        df = indexer.fit(df).transform(df);

        OneHotEncoderEstimator oneHotEncoderEstimator = new OneHotEncoderEstimator()
                                                            .setInputCols(new String[]{"Gen"})
                                                            .setOutputCols(new String[]{"GenderVector"});

        df = oneHotEncoderEstimator.fit(df).transform(df);
        String []features = {"GenderVector","Age","Height","Weight"};

        VectorAssembler assembler = new VectorAssembler()
                                        .setInputCols(features)
                                        .setOutputCol("features");

        df = assembler.transform(df);

        LinearRegression linearRegression = new LinearRegression();
        LinearRegressionModel linearRegressionModel = linearRegression.fit(df);
        df = linearRegressionModel.transform(df);
        df.show();


    }

}
