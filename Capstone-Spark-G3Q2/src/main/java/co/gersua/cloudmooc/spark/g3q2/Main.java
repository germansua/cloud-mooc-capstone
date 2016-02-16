package co.gersua.cloudmooc.spark.g3q2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

public class Main {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final List<String> FILTER = Arrays.asList("CMI->ORD->LAX");

    public static void main(String[] args) {

        if (args.length < 4) {
            System.err.println("Usage: Main <input file> <output location> <CRS or Actual> <filter enable>");
            System.exit(1);
        }

        final boolean applyFilter = Boolean.valueOf(args[3]);
        final boolean crsEnabled = args[3].equalsIgnoreCase("CRS");

        SparkConf conf = new SparkConf().setAppName("Tom wants to travel from airport X to airport Z. " +
                "However, Tom also wants to stop at airport Y for some sightseeing on the way.");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Point to the file
        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaPairRDD<String, FlightInfo> keysCreation = lines.flatMapToPair(line -> {

            String[] values = line.split("\t");
            if (values.length >= 7) {
                String origin = values[0].replace("\"", "");
                String dest = values[1].replace("\"", "");
                Date flightDate = DATE_FORMAT.parse(values[2].replace("\"", ""));
                String flightNum = values[3].replace("\"", "");
                int crsDepTime = Integer.valueOf(values[4].replace("\"", ""));
                int depTime = Integer.valueOf(values[5].replace("\"", ""));
                double arrDelay = Double.valueOf(values[6].replace("\"", ""));

                Calendar calendarOrg = getCalendar(flightDate);
                calendarOrg.add(Calendar.DATE, 2);
                String orgKey = String.format("%s->%s:%s:%s", origin, dest, DATE_FORMAT.format(calendarOrg.getTime()), String.valueOf(true));
                FlightInfo orgFlightInfo = new FlightInfo(origin, dest, flightDate, flightNum, crsDepTime, depTime, arrDelay, true);

                Calendar calendarDst = getCalendar(flightDate);
                String destKey = String.format("%s->%s:%s:%s", dest, origin, DATE_FORMAT.format(calendarDst.getTime()), String.valueOf(false));
                FlightInfo destFlightInfo = new FlightInfo(origin, dest, flightDate, flightNum, crsDepTime, depTime, arrDelay, false);

                List<Tuple2<String, FlightInfo>> tupleList = new ArrayList<>();
                int departureTimeFilter = crsEnabled ? crsDepTime : depTime;
                if (departureTimeFilter == 1200) {
                    // Both
                    tupleList.add(new Tuple2<>(orgKey, orgFlightInfo));
                    tupleList.add(new Tuple2<>(destKey, destFlightInfo));
                } else if (departureTimeFilter <= 1200) {
                    // Only Origin
                    tupleList.add(new Tuple2<>(orgKey, orgFlightInfo));
                } else if (departureTimeFilter >= 1200) {
                    // Only Dest
                    tupleList.add(new Tuple2<>(destKey, destFlightInfo));
                }

                return tupleList;
            }

            return Collections.emptyList();
        });

        JavaPairRDD<String, FlightInfo> getMinimalDelays = keysCreation.reduceByKey((fInfo1, fInfo2) -> {
            if (fInfo1.getArrDelay() <= fInfo2.getArrDelay()) {
                return fInfo1;
            } else {
                return fInfo2;
            }
        });

        getMinimalDelays.mapToPair(pair -> {
            String[] values = pair._1().split(":");
            String[] keys = values[0].split("->");
            String joinKey = pair._2().isOriginKey() ? keys[1] : keys[0];
            String date = values[1];
            return new Tuple2<>(String.format("%s:%s", joinKey, date), pair._2());
        }).groupByKey().flatMapToPair(pair -> {
            List<FlightInfo> orgList = new ArrayList<>();
            List<FlightInfo> destList = new ArrayList<>();

            String dateKey = pair._1();
            for (FlightInfo flightInfo : pair._2()) {
                if (flightInfo.isOriginKey()) {
                    orgList.add(flightInfo);
                } else {
                    destList.add(flightInfo);
                }
            }

            List<Tuple2<String, List<FlightInfo>>> results = new ArrayList<>();
            if (!orgList.isEmpty() && !destList.isEmpty()) {
                for (FlightInfo orgFI : orgList) {
                    for (FlightInfo dstFI : destList) {
                        String key = String.format("%s->%s->%s", dstFI.getDest(), dstFI.getOrigin(), orgFI.getDest());
                        if (applyFilter) {
                            if (FILTER.contains(key)) {
                                results.add(new Tuple2<>(key, Arrays.asList(orgFI, dstFI)));
                            }
                        } else {
                            results.add(new Tuple2<>(key, Arrays.asList(orgFI, dstFI)));
                        }
                    }
                }
            }

            return results;
        }).foreach(tuple -> System.out.println("Pair: " + tuple._1() + ", Values: " + tuple._2()));
    }

    public static final Calendar getCalendar(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar;
    }
}
