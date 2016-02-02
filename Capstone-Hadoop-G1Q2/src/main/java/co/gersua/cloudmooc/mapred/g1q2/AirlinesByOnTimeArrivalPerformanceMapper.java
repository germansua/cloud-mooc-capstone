package co.gersua.cloudmooc.mapred.g1q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirlinesByOnTimeArrivalPerformanceMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] data = value.toString().split("\\s");

        if (data.length >= 2) {
            try {
                String airlineId = data[0];
                double arrDelayMinutes = Double.valueOf(data[1]);
                context.write(new Text(airlineId), new DoubleWritable(arrDelayMinutes));
            } catch (Exception ex) {
                System.out.println("*** EXCEPTION - VALUE READ: \"" + value.toString() + "\"; Key: \"" + key.toString() + "\" ***");
            }
        } else {
            System.out.println("*** EXCEPTION - VALUE READ: \"" + value.toString() + "\"; Key: \"" + key.toString() + "\" ***");
        }
    }
}
