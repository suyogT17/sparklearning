package com.sparklearning.Practicals.sparkml;

import com.sparklearning.sparksql.SparkSqlApp;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;
import static org.apache.spark.sql.functions.*;

public class FreeTrialsDecisionTree {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        Dataset<Row> df = sparkSession.read().option("header", true).option("inferSchema", true).format("csv").load("src/main/resources/datasets/vppFreeTrials.csv");

        sparkSession.udf().register("countryUdf",(String country)->{

            List<String> topCountries =  Arrays.asList(new String[] {"GB","US","IN","UNKNOWN"});
            List<String> europeanCountries =  Arrays.asList(new String[] {"BE","BG","CZ","DK","DE","EE","IE","EL","ES","FR","HR","IT","CY","LV","LT","LU","HU","MT","NL","AT","PL","PT","RO","SI","SK","FI","SE","CH","IS","NO","LI","EU"});

            if (topCountries.contains(country)) return country;
            if (europeanCountries .contains(country)) return "EUROPE";
            else return "OTHER";
        }, DataTypes.StringType);

        df = df.withColumn("country",callUDF("countryUdf",col("country")))
                .withColumn("label",when(col("payments_made").$greater(0),1).otherwise(0));

        StringIndexer countryIndexer = new StringIndexer()
                                        .setInputCol("country")
                                        .setOutputCol("countryIndexer");
        df = countryIndexer.fit(df).transform(df);

        StringIndexer rebillIndexer = new StringIndexer()
                                        .setInputCol("rebill_period")
                                        .setOutputCol("rebillIndexer");
        df = rebillIndexer.fit(df).transform(df);

        //for decision tree we dont need to do one hot encoding

        Dataset<Row> countryIndexes = df.select("countryIndexer").distinct();

        //Indexes to String
        IndexToString indexToString = new IndexToString()
                .setInputCol("countryIndexer")
                .setOutputCol("countryName");
        Dataset<Row> countryIndexDf =  indexToString.transform(countryIndexes);
        countryIndexDf.show();

        VectorAssembler assembler = new VectorAssembler()
                                    .setInputCols(new String[]{"chapter_access_count","seconds_watched","countryIndexer","rebillIndexer"})
                                    .setOutputCol("features");
        df = assembler.transform(df);

        Dataset<Row> inputDataset = df.select("label","features");

        Dataset<Row>[] splitDf = inputDataset.randomSplit(new double[]{.8,.2});
        Dataset<Row> trainingAndTestDf = splitDf[0];   //train data
        Dataset<Row> holdOutDf = splitDf[1];    //holdout data


        //Using DecisionTreeRegressor
        /*DecisionTreeRegressor regressor = new DecisionTreeRegressor();
        regressor.setMaxDepth(3);

        DecisionTreeRegressionModel model = regressor.fit(trainingAndTestDf);

        model.transform(holdOutDf).show();
*/
        //Using DecisionTreeClassifier
        /*DecisionTreeClassifier regressor = new DecisionTreeClassifier();
        regressor.setMaxDepth(3);

        DecisionTreeClassificationModel model = regressor.fit(trainingAndTestDf);
*/

        RandomForestClassifier regressor = new RandomForestClassifier();
        regressor.setMaxDepth(5);

        RandomForestClassificationModel model = regressor.fit(trainingAndTestDf);

        Dataset<Row> predictionss = model.transform(holdOutDf);
        predictionss.show();

        System.out.println(model.toDebugString());

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
        evaluator.setMetricName("accuracy");
        System.out.println(evaluator.evaluate(predictionss));

        //df.show();
    }
}
