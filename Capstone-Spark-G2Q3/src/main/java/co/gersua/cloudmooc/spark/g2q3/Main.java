package co.gersua.cloudmooc.spark.g2q3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class Main {

    private static final List<String> KEY_FILTER = Arrays.asList("CMI->ORD", "IND->CMH", "DFW->IAH", "LAX->SFO", "JFK->LAX", "ATL->PHX");

    public static void main(String[] args) {

        if (args.length < 3) {
            System.err.println("Usage: Main <input file> <output location> <filter enable>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("For each source-destination pair X-Y, rank the top-10 carriers in " +
                "decreasing order of on-time arrival performance at Y from X.");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final boolean applyFilter = Boolean.valueOf(args[2]);

        // Point to the file
        JavaRDD<String> lines = sc.textFile(args[0]);

        // Generate Pair OrgDestCarrier as Key, Delay and Count as value
        JavaPairRDD<String, AverageWrapper> orgDestCarrier = lines.flatMapToPair(line -> {
            String[] values = line.split("\t");

            if (values.length >= 4) {
                String key = String.format("%s->%s:%s",
                        values[0].replace("\"", ""), values[1].replace("\"", ""), values[2].replace("\"", ""));
                Double arrDelay = Double.valueOf(values[3].replace("\"", ""));
                AverageWrapper aw = new AverageWrapper(arrDelay, 1);
                return Arrays.asList(new Tuple2<>(key, aw));
            }

            return Collections.emptyList();
        });

        // Reduce previous generated Pairs
        JavaPairRDD<String, AverageWrapper> orgDestCarrierAVG = orgDestCarrier.reduceByKey((aw1, aw2) ->
                new AverageWrapper(
                        aw1.getValue() + aw2.getValue(),
                        aw1.getCount() + aw2.getCount())
        );

        // Generate new pairs where Org/Dest are the keys
        JavaPairRDD<String, CarrierDelay> keyPairs = orgDestCarrierAVG.flatMapToPair(tuple -> {
            String oldKey = tuple._1();
            AverageWrapper aw = tuple._2();

            String[] values = oldKey.split(":");
            String orgDestKey = values[0];
            String carrier = values[1];
            CarrierDelay carrierDelay = new CarrierDelay(carrier, aw.getValue(), aw.getCount());

            if (applyFilter) {
                if (KEY_FILTER.contains(orgDestKey)) {
                    return Arrays.asList(new Tuple2<>(orgDestKey, carrierDelay));
                }
                return Collections.emptyList();
            }

            return Arrays.asList(new Tuple2<>(orgDestKey, carrierDelay));
        });

        // Group by Key and filter top 10 results by key
        JavaPairRDD<String, CarrierDelay> top10 = keyPairs.groupByKey().sortByKey().flatMapToPair(pair -> {
            String orgDestKey = pair._1();
            Iterable<CarrierDelay> carrierDelays = pair._2();

            TreeSet<CarrierDelay> carrierDelaySet = new TreeSet<>(
                    (c1, c2) -> Double.valueOf(c1.getDelay() / c1.getCount()).compareTo(c2.getDelay() / c2.getCount()));

            for (CarrierDelay cd : carrierDelays) {
                carrierDelaySet.add(cd);
                if (carrierDelaySet.size() > 10) {
                    carrierDelaySet.pollLast();
                }
            }

            List<Tuple2<String, CarrierDelay>> finalTuples = new ArrayList<>();
            for (CarrierDelay carrierDelay : carrierDelaySet) {
                finalTuples.add(new Tuple2<>(orgDestKey, carrierDelay));
            }

            return finalTuples;
        });

        top10.saveAsTextFile(args[1]);
    }
}
