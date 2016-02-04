package co.gersua.cloudmooc.mapred.g3q2;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BestFlightReducer extends Reducer<Text, Text, Text, Text> {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        FlightInfo bestOrgPerformance = null;
        FlightInfo bestDstPerformance = null;

        for (Text value : values) {
            FlightInfo flightInfo;
            try {
                flightInfo = new FlightInfo(key.toString(), value.toString());
            } catch (FlightException ex) {
                continue;
            }

            if (flightInfo.getType().equals("ORG")) {
                if (bestOrgPerformance == null || flightInfo.getArrivalDelay() < bestOrgPerformance.getArrivalDelay()) {
                    bestOrgPerformance = flightInfo;
                }
            } else {
                if (bestDstPerformance == null || flightInfo.getArrivalDelay() < bestDstPerformance.getArrivalDelay()) {
                    bestDstPerformance = flightInfo;
                }
            }
        }

        if (bestOrgPerformance == null || bestDstPerformance == null ||
                bestOrgPerformance.getValueAirport().equals(bestDstPerformance.getValueAirport())) {
            return;
        }

        String outputKey = String.format("%s->%s->%s",
                bestDstPerformance.getValueAirport(),
                bestDstPerformance.getKeyAirport(),
                bestOrgPerformance.getValueAirport()
        );

        String outputValue = String.format("%s\t%d\t%d\t%s\t%d\t%d",
                DATE_FORMAT.format(bestOrgPerformance.getFlightDate()),
                bestOrgPerformance.getDepartureTime(),
                bestOrgPerformance.getArrivalDelay(),
                DATE_FORMAT.format(bestDstPerformance.getFlightDate()),
                bestDstPerformance.getDepartureTime(),
                bestDstPerformance.getArrivalDelay()
        );

        context.write(new Text(outputKey), new Text(outputValue));
    }
}
