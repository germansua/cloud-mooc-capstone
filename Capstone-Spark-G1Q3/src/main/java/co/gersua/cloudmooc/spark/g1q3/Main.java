package co.gersua.cloudmooc.spark.g1q3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

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

        JavaPairRDD<String, Double> dayArrivalDelayPair = lines.flatMapToPair(line -> {
            String[] splitLine = line.split(SPLIT_PATTERN);
            String key = splitLine.length == 0 ? "" : splitLine[0];
            Double value = splitLine.length < 2 ? value = 0.0 : Double.valueOf(splitLine[1]);
            return Arrays.asList(new Tuple2<>(key, value));
        });

        JavaPairRDD<String, AverageWrapper> dayAverageWrapper =
                dayArrivalDelayPair.mapValues(value -> new AverageWrapper(value, 1));

        JavaPairRDD<String, AverageWrapper> daysValueCount = dayAverageWrapper.reduceByKey((aw1, aw2) ->
                new AverageWrapper(aw1.getValue() + aw2.getValue(), aw1.getCount() + aw2.getCount()));

        Map<String, AverageWrapper> resultMap = daysValueCount.collectAsMap();
        List<Map.Entry<String, AverageWrapper>> listResults = new ArrayList<>();
        listResults.addAll(resultMap.entrySet());
        Collections.sort(listResults, (entry1, entry2) ->
                Double.valueOf(entry1.getValue().getValue()).compareTo(entry2.getValue().getValue()));

        for (Map.Entry<String, AverageWrapper> entry : listResults) {
            System.out.printf("%s -> (%f, %d)\n",
                    entry.getKey(), entry.getValue().getValue(), entry.getValue().getCount());
        }

//        JavaPairRDD<String, Double> resultRDD =
//                daysValueCount.mapValues(averageWrapper -> averageWrapper.getValue() / averageWrapper.getCount());
//
//        Map<String, Double> results = resultRDD.collectAsMap();


//        List<Map.Entry<String, Double>> listResults = new ArrayList<>();
//        listResults.addAll(results.entrySet());
//        Collections.sort(listResults, (entry1, entry2) -> entry1.getValue().compareTo(entry2.getValue()));
//
//        for (Map.Entry<String, Double> entry : listResults) {
//            System.out.printf("%s:\t%f\n", entry.getKey(), entry.getValue());
//        }
    }
}
