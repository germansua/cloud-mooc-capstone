package co.gersua.cloudmooc.mapred.g1q2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeSet;

public class Top10AirlinesMapper extends Mapper<Text, Text, NullWritable, TextArrayWritable> {

    private TreeSet<Pair<Double, String>> topCounter = new TreeSet<Pair<Double, String>>();

    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String airline = key.toString();
        Double airlineDelay = Double.valueOf(value.toString());

        topCounter.add(new Pair<Double, String>(airlineDelay, airline));
        if (topCounter.size() > 10) {
            topCounter.remove(topCounter.last());
        }
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
