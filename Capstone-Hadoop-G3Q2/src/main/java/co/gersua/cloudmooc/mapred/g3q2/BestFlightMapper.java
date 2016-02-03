package co.gersua.cloudmooc.mapred.g3q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BestFlightMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] data = line.split("\\s");

        if (data.length >= 5) {
            String originAirport = data[0];
            String destAirport = data[1];
            String flightDate = data[2];
            String departureTime = data[3];
            String arrivalDelay = data[4];

            String originValue =
                    String.format("%s\t%s\t%s\t%s\t%s", destAirport, flightDate, departureTime, arrivalDelay, "ORG");
            String destValue =
                    String.format("%s\t%s\t%s\t%s\t%s", originAirport, flightDate, departureTime, arrivalDelay, "DST");

            context.write(new Text(originAirport), new Text(originValue));
            context.write(new Text(destAirport), new Text(destValue));
        }
    }
}
