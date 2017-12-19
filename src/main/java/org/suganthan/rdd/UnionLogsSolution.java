package org.suganthan.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Created by msuganthan on 17/12/17.
 */
public class UnionLogsSolution {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setAppName("unionLogs").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> julyFirstLogs = javaSparkContext.textFile("nasa_19950701.tsv");
        JavaRDD<String> augustFirstLogs = javaSparkContext.textFile("nasa_19950801.tsv");

        JavaRDD<String> aggregatedLogLines = julyFirstLogs.union(augustFirstLogs);
        System.out.println("aggregated count "+ aggregatedLogLines.count());
        JavaRDD<String> cleanLogLines = aggregatedLogLines.filter(line -> isNotHeader(line));

        System.out.println("clean logs count "+ cleanLogLines.count());

        JavaRDD<String> sampleLogs = cleanLogLines.sample(true, 0.1);

        System.out.println("sample logs "+sampleLogs.count());

        sampleLogs.saveAsTextFile("sample_nasa_file.text");
    }

    private static boolean isNotHeader(String line) {
        return !(line.startsWith("host") && line.contains("bytes"));
    }
}
