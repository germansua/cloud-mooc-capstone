package co.gersua.cloudmooc.mapred.g1q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirlinesByOnTimeArrivalPerformanceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text airlineId, Iterable<DoubleWritable> delayMinutes, Context context)
            throws IOException, InterruptedException {

        double totalDelay = 0.0;
        for (DoubleWritable delay : delayMinutes) {
            totalDelay += delay.get();
        }
        context.write(airlineId, new DoubleWritable(totalDelay));
    }
}
