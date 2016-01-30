package co.gersua.cloudmooc.mapred.g1q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class Top10AirlinesReducer extends Reducer<NullWritable, TextArrayWritable, Text, DoubleWritable> {

    private TreeSet<Pair<Double, String>> topCounter = new TreeSet<Pair<Double, String>>();

    @Override
    public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context)
            throws IOException, InterruptedException {

        for (TextArrayWritable value : values) {
            Text[] pairs = (Text[]) value.toArray();
            String airline = pairs[0].toString();
            Double delay = Double.valueOf(pairs[1].toString());

            topCounter.add(new Pair<Double, String>(delay, airline));
            if (topCounter.size() > 10) {
                topCounter.remove(topCounter.first());
            }
        }

        for (Pair<Double, String> item : topCounter) {
            Text airport = new Text(item.y);
            DoubleWritable totalDelay = new DoubleWritable(item.x);
            context.write(airport, totalDelay);
        }
    }
}
