package co.gersua.cloudmooc.spark.g1q1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Main {

    private static final String SPLIT_PATTERN = "\\s";

    public static void main(String[] args) {

        if (args.length == 0) {
            System.err.println("Usage: Main <file>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("Top 10 most popular airports by numbers of flights to/from the airport");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaRDD<String> airports = lines.flatMap(line -> Arrays.asList(line.split(SPLIT_PATTERN)));
        JavaPairRDD<String, Long> airportsMap =
                airports.mapToPair(airport -> new Tuple2<>(airport.replaceAll("\"", ""), 1L));
        JavaPairRDD<String, Long> airportsReduce =
                airportsMap.reduceByKey((accumulatedVal, newVal) -> accumulatedVal + newVal);
        List<Tuple2<String, Long>> top10 = airportsReduce.takeOrdered(10, new SerializableComparator<>());
        top10.forEach(tuple -> System.out.printf("%s\t%d\n", tuple._1(), tuple._2()));
    }
}
