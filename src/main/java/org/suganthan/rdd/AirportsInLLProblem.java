package org.suganthan.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by msuganthan on 17/12/17.
 */
public class AirportsInLLProblem {
    public static void main(String[] args) {
        // a regular expression which matches commas but not commas within double quotations
        final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

        SparkConf sparkConf = new SparkConf().setAppName("AirportsInUsa").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> airportsInUSA = javaSparkContext.textFile("airports.text")
                .filter(line -> Float.valueOf(line.split(COMMA_DELIMITER)[6]) > 40);

        JavaRDD<String> airportsNameAndCityNames = airportsInUSA.map(line -> {
            String[] splits = line.split(COMMA_DELIMITER);
            return String.join(", ", new String[] {splits[1], splits[6]});
        });

        airportsNameAndCityNames.saveAsTextFile("airports_with_latitude.text");
    }
}
