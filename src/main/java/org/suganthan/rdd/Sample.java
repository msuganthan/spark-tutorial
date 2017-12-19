package org.suganthan.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by msuganthan on 17/12/17.
 */
public class Sample {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> integerJavaRDD = javaSparkContext.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9));

    }
}
