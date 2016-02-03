package co.gersua.cloudmooc.mapred.g2q1q2.all;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeSet;

public class Top10CarriersOnTimeMapper extends Mapper<Text, Text, NullWritable, TextArrayWritable> {

    private TreeSet<Pair<Double, String>> topCounter = new TreeSet<Pair<Double, String>>();

    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String airportCarrier = key.toString();
        Double departureDelay = Double.valueOf(value.toString());
        topCounter.add(new Pair<Double, String>(departureDelay, airportCarrier));
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        for (Pair<Double, String> pair : topCounter) {
            String[] strings = {pair.y, pair.x.toString()};
            TextArrayWritable val = new TextArrayWritable(strings);
            context.write(NullWritable.get(), val);
        }
    }
}
