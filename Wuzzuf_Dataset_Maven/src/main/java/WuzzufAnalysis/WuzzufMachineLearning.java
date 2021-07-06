package WuzzufAnalysis;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.SparkSession;


public class WuzzufMachineLearning {
    String path;
    final SparkSession sparkSession;
    final DataFrameReader dataFrameReader;
    Dataset<Row> trainingData;

    public WuzzufMachineLearning(String path) {
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("org").setLevel(Level.ERROR);
        this.path = path;
        this.sparkSession = SparkSession.builder().appName("wuzzufml")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .master("local").getOrCreate();
        this.sparkSession.sparkContext().setLogLevel("ERROR");
        this.dataFrameReader = this.sparkSession.read().option("header", true);
        this.trainingData = this.dataFrameReader.csv(this.path);


    }

    public void SampleSummeryStructure() {
        System.out.println("Number of  rows before removing nulls and duplicates : "+this.trainingData.count());
        this.trainingData=this.trainingData.distinct();
        this.trainingData.na().drop();
        System.out.println("Number of  rows after removing nulls and duplicates : "+this.trainingData.count());
        this.trainingData.show(5);
        this.trainingData.describe().show();
        this.trainingData.printSchema();


    }


}
