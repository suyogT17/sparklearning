package com.sparklearning.Practicals.sparkml;

import com.sparklearning.sparksql.SparkSqlApp;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
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
import static org.apache.spark.sql.functions.*;

public class LRHousePrice {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        Dataset<Row> df = sparkSession.read().option("header",true).option("inferSchema",true).format("csv").load("src/main/resources/datasets/kc_house_data.csv");
        df = df.withColumnRenamed("price","label");
        df = df.withColumn("sqft_above_percentage", col("sqft_above").divide(col("sqft_living")));

        //df.select("label","bedrooms","bathrooms","sqft_living","sqft_above_percentage","floors");
        //traindata,,testdata(used by spark),holdout data
        Dataset<Row>[] splitDf = df.randomSplit(new double[]{.8,0.2});
        Dataset<Row> trainingAndTestDf = splitDf[0];   //train data
        Dataset<Row> holdOutDf = splitDf[1];    //holdout data

        //df.show();

        //Building Pipeline

        StringIndexer conditionIndexer = new StringIndexer().setInputCol("condition").setOutputCol("conditionIndex");
        //df = conditionIndexer.fit(df).transform(df);

        StringIndexer gradeIndexer  = new StringIndexer().setInputCol("grade").setOutputCol("gradeIndex");
        //df = gradeIndexer.fit(df).transform(df);

        StringIndexer zipCodeIndexer = new StringIndexer().setInputCol("zipcode").setOutputCol("zipcodeIndex");
        //df = zipCodeIndexer.fit(df).transform(df);

        StringIndexer waterFrontIndexer = new StringIndexer().setInputCol("waterfront").setOutputCol("waterfrontIndex");
        //df = waterFrontIndexer.fit(df).transform(df);

        //OneHotEncoding
        OneHotEncoderEstimator oneHotEncoderEstimator = new OneHotEncoderEstimator()
                                                            .setInputCols(new String[]{"conditionIndex","gradeIndex","zipcodeIndex"})
                                                            .setOutputCols(new String[]{"conditionVector","gradeVector","zipcodeVector"});

        //df = oneHotEncoderEstimator.fit(df).transform(df);
        //df.show();

        String[] features = {"bedrooms","bathrooms","sqft_living","sqft_above_percentage","floors","conditionVector","gradeVector","zipcodeVector","waterfrontIndex"};
        VectorAssembler assembler = new VectorAssembler()
                                        .setInputCols(features)
                                        .setOutputCol("features");

        //df = assembler.transform(df);
        //df = df.select("label","features");
        //df.show(false);

        LinearRegression linearRegression = new LinearRegression();

        ParamGridBuilder paramGridBuilder = new ParamGridBuilder();

        ParamMap[] paramMaps = paramGridBuilder.addGrid(linearRegression.regParam(),new double[]{0.01,0.1,0.5})
                                                .addGrid(linearRegression.elasticNetParam(),new double[]{0,0.5,1})
                                                .build();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                                                            .setEstimator(linearRegression)
                                                            .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                                                            .setEstimatorParamMaps(paramMaps)
                                                            .setTrainRatio(.8);

        //Building Pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{conditionIndexer,gradeIndexer,zipCodeIndexer,waterFrontIndexer,oneHotEncoderEstimator,assembler,trainValidationSplit});


        PipelineModel pipelineModel = pipeline.fit(trainingAndTestDf);
        TrainValidationSplitModel trainValidationSplitModel = (TrainValidationSplitModel) pipelineModel.stages()[6];


        //TrainValidationSplitModel trainValidationSplitModel = trainValidationSplit.fit(trainingAndTestDf);
        LinearRegressionModel linearRegressionModel = (LinearRegressionModel)trainValidationSplitModel.bestModel();

        //linearRegressionModel.transform(holdOutDf);

        Dataset<Row> holdOutResult = pipelineModel.transform(holdOutDf);
        holdOutResult.show();
        holdOutResult = holdOutResult.drop("prediction");

        //outputdf.show(false);
        System.out.println(linearRegressionModel.summary().r2()+"  "+linearRegressionModel.summary().rootMeanSquaredError());
        System.out.println(linearRegressionModel.evaluate(holdOutResult).r2()+"  "+linearRegressionModel.evaluate(holdOutResult).rootMeanSquaredError());

        System.out.println(linearRegressionModel.getRegParam()+" "+linearRegression.getRegParam());
    }


}
