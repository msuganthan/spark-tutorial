package org.suganthan.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

/**
 * Created by msuganthan on 24/12/17.
 */
public class AverageHousePriceSolutionReduceByKey {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("averageHousePriceSolution").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("RealEstate.csv");
        JavaRDD<String> cleanedLines = lines.filter(line -> !line.contains("Bedrooms"));

        JavaPairRDD<String, AvgCount> housePriceRdd = cleanedLines.mapToPair(line ->
                new Tuple2<>(line.split(",")[3],
                        new AvgCount(1, Double.parseDouble(line.split(",")[2]))));

        JavaPairRDD<String, AvgCount> houseTotalPrice = housePriceRdd.reduceByKey((x, y) ->
            new AvgCount(x.getCount()+ y.getCount(), x.getTotal() + y.getTotal()));

        JavaPairRDD<String, Double> housePriceAvg =  houseTotalPrice.mapValues(avgCount -> avgCount.getTotal() / avgCount.getCount());
        for (Map.Entry<String, Double> housePriceAvgPair : housePriceAvg.collectAsMap().entrySet()) {
            System.out.println(housePriceAvgPair.getKey() +" : "+housePriceAvgPair.getValue());
        }
    }
}
