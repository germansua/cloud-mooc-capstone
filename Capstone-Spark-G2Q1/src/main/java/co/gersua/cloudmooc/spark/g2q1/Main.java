package co.gersua.cloudmooc.spark.g2q1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class Main {

    private static final List<String> AIRPORT_FILTER_TASK1 = Arrays.asList("CMI", "BWI", "MIA", "LAX", "IAH", "SFO");
    private static final List<String> AIRPORT_FILTER_TASK2 = Arrays.asList("SRQ", "CMH", "JFK", "SEA", "BOS");

    public static void main(String[] args) {

        if (args.length < 3) {
            System.err.println("Usage: Main <input file> <output location> <filter enable>");
//            System.err.println("Usage: Main <input file> <output location>");
            System.exit(1);
        }

        final boolean applyFilter = Boolean.valueOf(args[2]);

        SparkConf conf = new SparkConf().setAppName("For each airport X, rank the top-10 carriers in " +
                "decreasing order of on-time departure performance from X");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Lines
        JavaRDD<String> lines = sc.textFile(args[0]);

        // Origin airport along with carrier create a Map key, departure delay is the value
        JavaPairRDD<String, AverageWrapper> airportCarrierDelayMap = lines.flatMapToPair(line -> {
            String[] values = line.split("\t");

            if (values.length >= 3) {
                String originAirport = values[0].replace("\"", "");
                String carrier = values[1].replace("\"", "");
                Double departureDelay = Double.valueOf(values[2].replace("\"", ""));

                return Arrays.asList(new Tuple2<>(
                        String.format("%s:%s", originAirport, carrier),
                        new AverageWrapper(departureDelay, 1)
                ));
            }

            return Collections.emptyList();
        });

        // Reduce by key to get the sum of all Airport/carrier delays
        JavaPairRDD<String, AverageWrapper> airportCarrierReduceMap =
                airportCarrierDelayMap.reduceByKey((aw1, aw2) ->
                        new AverageWrapper(aw1.getValue() + aw2.getValue(), aw1.getCount() + aw2.getCount()));

        // Create a new Map where airport is the key
        JavaPairRDD<String, CarrierDelay> airportKey = airportCarrierReduceMap.flatMapToPair(tuple -> {
            String[] splitKey = tuple._1().split(":");
            String airport = splitKey[0];
            String carrier = splitKey[1];
            AverageWrapper aw = tuple._2();
            CarrierDelay carrierDelay = new CarrierDelay(carrier, aw.getValue(), aw.getCount());

            if (applyFilter) {
                if (AIRPORT_FILTER_TASK2.contains(airport)) {
                    return Arrays.asList(new Tuple2<>(airport, carrierDelay));
                }
                return Collections.emptyList();
            }
            return Arrays.asList(new Tuple2<>(airport, carrierDelay));
        });

        // Get top 10 results for each Airport/Carrier Delay
        JavaPairRDD<String, CarrierDelay> top10 = airportKey.groupByKey().sortByKey(true).flatMapToPair(tuple -> {
            TreeSet<CarrierDelay> orderedResults = new TreeSet<>(
                    (c1, c2) -> Double.valueOf(c1.getDelay() / c1.getCount()).compareTo(c2.getDelay() / c2.getCount())
            );

            String key = tuple._1();
            Iterable<CarrierDelay> carrierDelays = tuple._2();
            for (CarrierDelay cd : carrierDelays) {
                orderedResults.add(cd);
                if (orderedResults.size() > 10) {
                    orderedResults.pollLast();
                }
            }

            List<Tuple2<String, CarrierDelay>> finalTuples = new ArrayList<>();
            for (CarrierDelay carrierDelay : orderedResults) {
                finalTuples.add(new Tuple2<>(key, carrierDelay));
            }

            return finalTuples;
        });

        top10.saveAsTextFile(args[1]);
    }
}
