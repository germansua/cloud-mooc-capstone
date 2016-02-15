package co.gersua.cloudmooc.spark.g2q2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class Main {

    private static final List<String> AIRPORT_FILTER = Arrays.asList("CMI", "BWI", "MIA", "LAX", "IAH", "SFO");

    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: Main <input file> <output location> <filter enable>");
//            System.err.println("Usage: Main <input file> <output location>");
            System.exit(1);
        }

        final boolean applyFilter = Boolean.valueOf(args[2]);

        SparkConf conf = new SparkConf().setAppName("For each airport X, rank the top-10 airports in " +
                "decreasing order of on-time departure performance from X");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Lines
        JavaRDD<String> lines = sc.textFile(args[0]);

        // Origin airport along with destination airport create a Map key, departure delay is the value
        JavaPairRDD<String, AverageWrapper> airportDestAirportDelayMap = lines.flatMapToPair(line -> {
            String[] values = line.split("\t");

            if (values.length >= 3) {
                String originAirport = values[0].replace("\"", "");
                String destAirport = values[1].replace("\"", "");
                Double departureDelay = Double.valueOf(values[2].replace("\"", ""));

                return Arrays.asList(new Tuple2<>(
                        String.format("%s:%s", originAirport, destAirport),
                        new AverageWrapper(departureDelay, 1)
                ));
            }

            return Collections.emptyList();
        });

        // Reduce by key to get the sum of all Airport/DestAirport delays
        JavaPairRDD<String, AverageWrapper> airportDestAirportReduceMap =
                airportDestAirportDelayMap.reduceByKey((aw1, aw2) ->
                        new AverageWrapper(aw1.getValue() + aw2.getValue(), aw1.getCount() + aw2.getCount()));

        // Create a new Map where airport is the key
        JavaPairRDD<String, DestAirportDelay> airportKey = airportDestAirportReduceMap.flatMapToPair(tuple -> {
            String[] splitKey = tuple._1().split(":");
            String airport = splitKey[0];
            String carrier = splitKey[1];
            AverageWrapper aw = tuple._2();
            DestAirportDelay destAirportDelay = new DestAirportDelay(carrier, aw.getValue(), aw.getCount());

            if (applyFilter) {
                if (AIRPORT_FILTER.contains(airport)) {
                    return Arrays.asList(new Tuple2<>(airport, destAirportDelay));
                }
                return Collections.emptyList();
            }
            return Arrays.asList(new Tuple2<>(airport, destAirportDelay));
        });

        // Get top 10 results for each Airport/DestAirport Delay
        JavaPairRDD<String, DestAirportDelay> top10 = airportKey.groupByKey().sortByKey(true).flatMapToPair(tuple -> {
            TreeSet<DestAirportDelay> orderedResults = new TreeSet<>(
                    (c1, c2) -> Double.valueOf(c1.getDelay() / c1.getCount()).compareTo(c2.getDelay() / c2.getCount())
            );

            String key = tuple._1();
            Iterable<DestAirportDelay> carrierDelays = tuple._2();
            for (DestAirportDelay cd : carrierDelays) {
                orderedResults.add(cd);
                if (orderedResults.size() > 10) {
                    orderedResults.pollLast();
                }
            }

            List<Tuple2<String, DestAirportDelay>> finalTuples = new ArrayList<>();
            for (DestAirportDelay carrierDelay : orderedResults) {
                finalTuples.add(new Tuple2<>(key, carrierDelay));
            }

            return finalTuples;
        });

        top10.saveAsTextFile(args[1]);
    }
}
