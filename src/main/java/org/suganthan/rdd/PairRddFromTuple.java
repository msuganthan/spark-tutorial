package org.suganthan.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by msuganthan on 19/12/17.
 */
public class PairRddFromTuple {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> tuple2 = Arrays.asList(new Tuple2<>("Lily", 23),
                new Tuple2<>("Jack", 23));
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(tuple2);
        pairRDD.coalesce(1).saveAsTextFile("pair_rdd_from_tuple_list");
    }
}
