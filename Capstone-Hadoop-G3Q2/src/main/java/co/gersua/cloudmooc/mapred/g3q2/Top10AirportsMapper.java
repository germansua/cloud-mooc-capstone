package co.gersua.cloudmooc.mapred.g3q2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeSet;

public class Top10AirportsMapper extends Mapper<Text, Text, NullWritable, TextArrayWritable> {

    private TreeSet<Pair<Integer, String>> topCounter = new TreeSet<Pair<Integer, String>>();

    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String airport = key.toString();
        Integer airportCount = Integer.valueOf(value.toString());

        topCounter.add(new Pair<Integer, String>(airportCount, airport));
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        for (Pair<Integer, String> pair : topCounter) {
            String[] strings = {pair.y, pair.x.toString()};
            TextArrayWritable val = new TextArrayWritable(strings);
            context.write(NullWritable.get(), val);
        }
    }
}
