package co.gersua.cloudmooc.mapred.g2q1q2.all;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirportCarrierDelayReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text airportCarrier, Iterable<DoubleWritable> delayMinutes, Context context)
            throws IOException, InterruptedException {

        double totalDelay = 0.0;
        for (DoubleWritable delay : delayMinutes) {
            totalDelay += delay.get();
        }
        context.write(airportCarrier, new DoubleWritable(totalDelay));
    }
}
