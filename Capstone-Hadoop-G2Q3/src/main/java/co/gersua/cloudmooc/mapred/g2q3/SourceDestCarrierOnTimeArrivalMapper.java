package co.gersua.cloudmooc.mapred.g2q3;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SourceDestCarrierOnTimeArrivalMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private static final List<String> allowedRoutes =
            Arrays.asList("\"CMI\":\"ORD\"", "\"IND\":\"CMH\"", "\"DFW\":\"IAH\"", "\"LAX\":\"SFO\"", "\"JFK\":\"LAX\"", "\"ATL\":\"PHX\"");

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] data = value.toString().split("\\s");

        if (data.length >= 4) {
            try {
                String srcAirport = data[0];
                String dstAirport = data[1];
                String route = String.format("%s:%s", srcAirport, dstAirport);

                String carrier = data[2];
                double onTimeArrivalPerformance = Double.valueOf(data[3]);

                if (allowedRoutes.contains(route)) {
                    String generatedKey = String.format("%s:%s", route, carrier);
                    context.write(new Text(generatedKey), new DoubleWritable(onTimeArrivalPerformance));
                }
            } catch (Exception ex) {
                System.out.println("*** EXCEPTION - VALUE READ: \"" + value.toString() + "\"; Key: \"" + key.toString() + "\" ***");
            }
        } else {
            System.out.println("*** VALUE READ: \"" + value.toString() + "\"; Key: \"" + key.toString() + "\" ***");
        }
    }
}
