package co.gersua.cloudmooc.mapred.g2q3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SourceDestCarrierOnTimeArrivalReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text sourceDestCarrier, Iterable<DoubleWritable> arrivalPerformanceMinutes, Context context)
            throws IOException, InterruptedException {

        double totalOnTimeArrivalPerformance = 0.0;
        for (DoubleWritable performance : arrivalPerformanceMinutes) {
            totalOnTimeArrivalPerformance += performance.get();
        }
        context.write(sourceDestCarrier, new DoubleWritable(totalOnTimeArrivalPerformance));
    }
}
