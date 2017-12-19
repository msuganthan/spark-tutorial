package org.suganthan.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by msuganthan on 17/12/17.
 */
public class SameHostProblem {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setAppName("unionLogs").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> julyFirstLogs = javaSparkContext.textFile("nasa_19950701.tsv");
        JavaRDD<String> augustFirstLogs = javaSparkContext.textFile("nasa_19950801.tsv");
        JavaRDD<String> julyFirstHosts = julyFirstLogs.map(line -> line.split("\t")[0]);

        JavaRDD<String> augustFirstHosts = augustFirstLogs.map(line -> line.split("\t")[0]);
        JavaRDD<String> intersection = julyFirstHosts.intersection(augustFirstHosts);

        JavaRDD<String> cleanedHostIntersection = intersection.filter(host -> !host.equals("host"));

        cleanedHostIntersection.saveAsTextFile("out/nasa_logs_same_hosts.csv");
    }
}
