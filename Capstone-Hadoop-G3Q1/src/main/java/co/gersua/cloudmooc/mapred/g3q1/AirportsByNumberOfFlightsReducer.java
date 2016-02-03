package co.gersua.cloudmooc.mapred.g3q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirportsByNumberOfFlightsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int airportCount = 0;
        for (IntWritable value : values) {
            airportCount += value.get();
        }
        context.write(key, new IntWritable(airportCount));
    }
}
