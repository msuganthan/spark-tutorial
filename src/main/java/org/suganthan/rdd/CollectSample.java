package org.suganthan.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by msuganthan on 17/12/17.
 */
public class CollectSample {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setAppName("unionLogs").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> languagesRDD = javaSparkContext.parallelize(Arrays.asList("java", "spark", "scala", "groovy", "python", "hadoop"));
        List<String> collectLanguage = languagesRDD.collect();

        collectLanguage.stream().forEach(System.out::println);

    }
}
