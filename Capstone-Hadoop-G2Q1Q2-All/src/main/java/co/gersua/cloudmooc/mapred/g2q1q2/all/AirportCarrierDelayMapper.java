package co.gersua.cloudmooc.mapred.g2q1q2.all;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AirportCarrierDelayMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] data = value.toString().split("\\s");

        if (data.length >= 3) {
            try {
                String airport = data[0];
                String carrier = data[1];
                double depDelayMinutes = Double.valueOf(data[2]);

                String generatedKey = String.format("%s:%s", airport, carrier);
                context.write(new Text(generatedKey), new DoubleWritable(depDelayMinutes));
            } catch (Exception ex) {
                System.out.println("*** EXCEPTION - VALUE READ: \"" + value.toString() + "\"; Key: \"" + key.toString() + "\" ***");
            }
        } else {
            System.out.println("*** VALUE READ: \"" + value.toString() + "\"; Key: \"" + key.toString() + "\" ***");
        }
    }
}
