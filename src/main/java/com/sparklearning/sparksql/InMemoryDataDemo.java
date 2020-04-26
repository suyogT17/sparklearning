package com.sparklearning.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class InMemoryDataDemo {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSqlApp.getSparkSession();

        /*
        * creating dataset from inMemory data
        * */
        List<Row> inMemory = new ArrayList<Row>();

        //RowFactory.create("WARN","16 December 2018") //returns the object of type of row we can add this to our list

        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

        StructField field[] = {new StructField("loglevel", DataTypes.StringType,false, Metadata.empty()),
                                new StructField("datetime", DataTypes.StringType,false, Metadata.empty())};
        StructType schema = new StructType(field);
        Dataset<Row> logsDataframe = sparkSession.createDataFrame(inMemory,schema);
        //logsDataframe.show();

        logsDataframe.createOrReplaceTempView("logs");
        Dataset<Row> logsCount = sparkSession.sql("select loglevel, collect_list(datetime) from logs group by loglevel");
        logsCount.show();

        sparkSession.close();
    }
}
