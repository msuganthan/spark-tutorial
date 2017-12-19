package org.suganthan.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by msuganthan on 17/12/17.
 */
public class AirportsInUsaProblem {
    public static void main(String[] args) {
        // a regular expression which matches commas but not commas within double quotations
        final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

        SparkConf sparkConf = new SparkConf().setAppName("AirportsInUsa").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> airportsInUSA = javaSparkContext.textFile("airports.text")
                .filter(line -> line.split(COMMA_DELIMITER)[3].equals("\"United States\""));

        JavaRDD<String> airportsNameAndCityNames = airportsInUSA.map(line -> {
            String[] splits = line.split(COMMA_DELIMITER);
            return String.join(", ", new String[] {splits[1], splits[2]});
        });

        airportsNameAndCityNames.saveAsTextFile("airports_in_usa.text");
    }
}
