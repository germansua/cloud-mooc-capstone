package co.gersua.cloudmooc.mapred.g3q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class BestFlightReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<FlightInfo> origins = new ArrayList<FlightInfo>();
        List<FlightInfo> destinations = new ArrayList<FlightInfo>();

        for (Text value : values) {
            FlightInfo flightInfo;
            try {
                flightInfo = new FlightInfo(key.toString(), value.toString());
            } catch (FlightException ex) {
                continue;
            }

            if (flightInfo.getType().equals("ORG")) {
                if (flightInfo.getDepartureTime() >= 1200) {
                    origins.add(flightInfo);
                }
            } else {
                if (flightInfo.getDepartureTime() <= 1200) {
                    destinations.add(flightInfo);
                }
            }
        }

        for (FlightInfo dest : destinations) {
            for (FlightInfo org : origins) {

                Calendar calendarOrg = Calendar.getInstance();
                calendarOrg.setTime(org.getFlightDate());

                Calendar calendarDest = Calendar.getInstance();
                calendarDest.setTime(dest.getFlightDate());

//                calendarDest.
//
//                if (dest.getDepartureTime())
            }
        }
    }
}
