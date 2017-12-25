package org.suganthan.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by msuganthan on 20/12/17.
 */
public class AirPortsNotInUsa {

    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("airports.text");

        JavaPairRDD<String, String> airportsNotInUsa = textFile.mapToPair(getAirportNameAndCountryNamePair())
                .filter(keyValue -> !keyValue._2().equals("\"United States\""));

        //map values
        airportsNotInUsa.mapValues(countryName -> countryName.toUpperCase());
        airportsNotInUsa.saveAsTextFile("airport_not_in_usa_pair");

    }

    private static PairFunction<String, String, String> getAirportNameAndCountryNamePair() {
        final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
        return line -> new Tuple2<>(line.split(COMMA_DELIMITER)[1], line.split(COMMA_DELIMITER)[3]);
    }
}
