package co.gersua.cloudmooc.spark.g3q2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

public class Main {

    private static final List<String> FILTER_TASK1 = Arrays.asList("CMI->ORD->LAX:2008-03-06", "JAX->DFW->CRP:2008-09-11",
            "SLC->BFL->LAX:2008-04-03", "LAX->SFO->PHX:2008-07-14", "DFW->ORD->DFW:2008-06-12", "LAX->ORD->JFK:2008-01-03");

    public static void main(String[] args) {

        if (args.length < 4) {
            System.err.println("Usage: Main <input file> <output location> <CRS or Actual> <filter enable>");
            System.exit(1);
        }

        final boolean applyFilter = Boolean.valueOf(args[3]);
        final boolean crsEnabled = args[3].equalsIgnoreCase("CRS");

        SparkConf conf = new SparkConf().setAppName("Tom wants to travel from airport X to airport Z. "
                + "However, Tom also wants to stop at airport Y for some sightseeing on the way.");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Point to the file
        JavaRDD<String> lines = sc.textFile(args[0]);

        lines.flatMapToPair(line -> {
            List<Tuple2<String, FlightInfo>> partialResults = new ArrayList<>();

            String[] values = line.split("\t");
            if (values.length >= 7) {
                // simpleDateFormat is here because it is not thread-safe
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

                String origin = values[0].replace("\"", "");
                String dest = values[1].replace("\"", "");
                Date flightDate = simpleDateFormat.parse(values[2]);
                String flightNum = values[3].replace("\"", "");
                int crsDepTime = Integer.valueOf(values[4].replace("\"", ""));
                int depTime = Integer.valueOf(values[5].replace("\"", ""));
                double arrDelay = Double.valueOf(values[6].replace("\"", ""));

                Calendar calendar = getCalendar(flightDate);
                String firstKey = String.format("%s:%s", origin, simpleDateFormat.format(calendar.getTime()));

                calendar.add(Calendar.DATE, 2);
                String secondKey = String.format("%s:%s", dest, simpleDateFormat.format(calendar.getTime()));

                partialResults.add(new Tuple2<>(firstKey,
                        new FlightInfo(origin, dest, flightDate, flightNum, crsDepTime, depTime, arrDelay, true)));

                partialResults.add(new Tuple2<>(secondKey,
                        new FlightInfo(origin, dest, flightDate, flightNum, crsDepTime, depTime, arrDelay, false)));
            }
            return partialResults;
        }).groupByKey().flatMapToPair(keyValue -> {
            List<Tuple2<String, AggregateFlightInfo>> partialResults = new ArrayList<>();
            List<FlightInfo> firstFlight = new ArrayList<>();
            List<FlightInfo> secondFlight = new ArrayList<>();

            keyValue._2().forEach(flightInfo -> {
                int departureTime = crsEnabled ? flightInfo.getCrsDepTime() : flightInfo.getDepTime();

                if (flightInfo.isOriginKey()) {
                    if (departureTime >= 1200) {
                        secondFlight.add(flightInfo);
                    }
                } else {
                    if (departureTime <= 1200) {
                        firstFlight.add(flightInfo);
                    }
                }
            });

            Map<String, AggregateFlightInfo> resultsMap = new HashMap<>();
            String[] values = keyValue._1().split(":");
            for (FlightInfo first : firstFlight) {
                for (FlightInfo second : secondFlight) {
                    String key = String
                            .format("%s->%s->%s:%s", first.getOrigin(), first.getDest(), second.getDest(), values[1]);
                    AggregateFlightInfo aggregateFlightInfo = new AggregateFlightInfo(first, second);

                    AggregateFlightInfo currentAggregateValue = resultsMap.get(key);
                    if (currentAggregateValue == null ||
                            aggregateFlightInfo.totalArrDelay() < currentAggregateValue.totalArrDelay()) {
                        resultsMap.put(key, aggregateFlightInfo);
                    }
                }
            }
            resultsMap.forEach((key, afi) -> {
                if (applyFilter) {
                    if (FILTER_TASK1.contains(key)) {
                        partialResults.add(new Tuple2<>(key, afi));
                    }
                } else {
                    partialResults.add(new Tuple2<>(key, afi));
                }
            });
            return partialResults;
        })

//                .reduceByKey((afi1, afi2) -> afi1.totalArrDelay() <= afi2.totalArrDelay() ? afi1 : afi2)

                .foreach(tuple -> System.out
                        .println("*******************************************\n" + tuple._1() + " : " + tuple._2()));
    }

    private static final Calendar getCalendar(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar;
    }
}
