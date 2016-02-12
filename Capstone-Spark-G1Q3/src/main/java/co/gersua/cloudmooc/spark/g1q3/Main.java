package co.gersua.cloudmooc.spark.g1q3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

public class Main {

    private static final String SPLIT_PATTERN = "\\s";

    public static void main(String[] args) {

        if (args.length == 0) {
            System.err.println("Usage: Main <file>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("Days of the week by on-time arrival performance");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaPairRDD<String, Double> dayAveragePair = lines.flatMapToPair(line -> {
            String[] splitLine = line.split(SPLIT_PATTERN);
            String key = splitLine.length == 0 ? "" : splitLine[1];
            Double value = splitLine.length < 2 ? value = 0.0 : Double.valueOf(splitLine[1]);
            return Arrays.asList(new Tuple2<>(key, value));
        });

        //https://www.safaribooksonline.com/library/view/learning-spark/9781449359034/ch04.html

//        dayAveragePair.combineByKey();
    }
}
