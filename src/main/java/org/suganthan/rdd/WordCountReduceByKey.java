package org.suganthan.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by msuganthan on 20/12/17.
 */
public class WordCountReduceByKey {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[1]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        JavaRDD<String> lines = javaSparkContext.textFile("word_count.text");
        JavaRDD<String> wordRdd = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordPairRdd = wordRdd.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordCounts = wordPairRdd.reduceByKey((x, y) -> x + y);
        for (Map.Entry<String, Integer> entry : wordCounts.collectAsMap().entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }
}
