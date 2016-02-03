package co.gersua.cloudmooc.mapred.g3q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
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
                origins.add(flightInfo);
            } else {
                destinations.add(flightInfo);
            }
        }


        for (FlightInfo dest : destinations) {
            for (FlightInfo org : origins) {
                
            }
        }
    }
}
