package com.sparklearning.Practicals.sparkml;

import com.sparklearning.sparksql.SparkSqlApp;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePriceFields {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        Dataset<Row> df = sparkSession.read().option("header",true).option("inferSchema",true).format("csv").load("src/main/resources/datasets/kc_house_data.csv");

        //df.describe().show();
        df = df.drop("id","date","waterfront","view","condition","grade","yr_renovated","zipcode","lat","long");

        for(String colName : df.columns()) {
            System.out.println("Correlation between price and "+colName+" : " + df.stat().corr("price", colName));
        }
        System.out.println("------------------------------------------------\n\n");
        df = df.drop("sqft_lot","sqft_lot15","sqft_living15","yr_built");

        for( String col1 :df.columns()){
            for(String col2: df.columns()){
                System.out.println("Correlation between "+ col1+" and "+col2+" : " + df.stat().corr(col1,col2));
            }
        }
    }

}
