package org.suganthan.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by msuganthan on 17/12/17.
 */
public class WordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        javaSparkContext.textFile("word_count.text")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .countByValue()
                .entrySet()
                .stream()
                .forEach(entry -> System.out.println(entry.getKey() + " ==> "+entry.getValue()));
    }
}
