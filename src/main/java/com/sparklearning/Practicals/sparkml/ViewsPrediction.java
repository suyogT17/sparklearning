package com.sparklearning.Practicals.sparkml;

import com.sparklearning.sparksql.SparkSqlApp;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import  static  org.apache.spark.sql.functions.*;

public class ViewsPrediction {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        Dataset<Row> df = sparkSession.read().option("header", true).option("inferSchema", true).format("csv").load("src/main/resources/datasets/viewsdatasets/*.csv");
       // df.printSchema();
        df = df.filter("is_cancelled = 'true'").drop("is_cancelled","observation_date");
        df = df.withColumn("firstSub",when(col("firstSub").isNull(),0).otherwise(col("firstSub")))
                .withColumn("all_time_views",when(col("all_time_views").isNull(),0).otherwise(col("all_time_views")))
                .withColumn("last_month_views",when(col("last_month_views").isNull(),0).otherwise(col("last_month_views")))
                .withColumn("next_month_views",when(col("next_month_views").isNull(),0).otherwise(col("next_month_views")));



        df = df.withColumnRenamed("next_month_views","label");
        StringIndexer paymentIndexer = new StringIndexer().setInputCol("payment_method_type").setOutputCol("paymentIndex");
        df = paymentIndexer.fit(df).transform(df);

        StringIndexer countryIndexer = new StringIndexer().setInputCol("country").setOutputCol("countryIndex");
        df = countryIndexer.fit(df).transform(df);

        StringIndexer rebillIndexer = new StringIndexer().setInputCol("rebill_period_in_months").setOutputCol("rebillIndex");
        df = rebillIndexer.fit(df).transform(df);

        OneHotEncoderEstimator oneHotEncoderEstimator = new OneHotEncoderEstimator()
                    .setInputCols(new String[]{"paymentIndex","countryIndex","rebillIndex"})
                    .setOutputCols(new String[]{"paymentVector","countryVector","rebillVector"});
        df = oneHotEncoderEstimator.fit(df).transform(df);

        VectorAssembler assembler = new VectorAssembler()
                                        .setInputCols(new String[]{"firstSub","age","all_time_views","last_month_views","paymentIndex","countryIndex","rebillIndex"})
                                        .setOutputCol("features");
        Dataset<Row> inputDataset = assembler.transform(df).select("label","features");

        inputDataset.printSchema();
        Dataset<Row>[] splitDf = inputDataset.randomSplit(new double[]{.9,0.1});
        Dataset<Row> trainingAndTestDf = splitDf[0];   //train data
        Dataset<Row> holdOutDf = splitDf[1];    //holdout data

        LinearRegression linearRegression  = new LinearRegression();

        ParamGridBuilder paramGridBuilder  = new ParamGridBuilder();
        ParamMap[] paramMaps = paramGridBuilder
                .addGrid(linearRegression.regParam(),new double[]{0.01,0.1,0.,0.5,0.7,0.9,1.0})
                .addGrid(linearRegression.elasticNetParam(),new double[]{0,0.5,1.0})
                .build();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(linearRegression)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                .setEstimatorParamMaps(paramMaps)
                .setTrainRatio(0.9);

        TrainValidationSplitModel trainValidationSplitModel = trainValidationSplit.fit(trainingAndTestDf);

        LinearRegressionModel linearRegressionModel = (LinearRegressionModel) trainValidationSplitModel.bestModel();

        linearRegressionModel.evaluate(holdOutDf).predictions().show();

       // inputDataset.show();

        System.out.println(linearRegressionModel.summary().r2()+"  "+linearRegressionModel.summary().rootMeanSquaredError());
        System.out.println(linearRegressionModel.evaluate(holdOutDf).r2()+"  "+linearRegressionModel.evaluate(holdOutDf).rootMeanSquaredError());

        System.out.println(linearRegressionModel.getRegParam()+" "+linearRegression.getRegParam());

    }

}
