package co.gersua.cloudmooc.mapred.g3q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class Top10AirportsReducer extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {

    private TreeSet<Pair<Integer, String>> topCounter = new TreeSet<Pair<Integer, String>>();
    private List<Pair<Integer, String>> rankList = new ArrayList<Pair<Integer, String>>();

    @Override
    public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context)
            throws IOException, InterruptedException {

        for (TextArrayWritable value : values) {
            Text[] pairs = (Text[]) value.toArray();
            String airport = pairs[0].toString();
            Integer count = Integer.valueOf(pairs[1].toString());
            topCounter.add(new Pair<Integer, String>(count, airport));
        }

        rankList.addAll(topCounter);
        Collections.sort(rankList, new Comparator<Pair<Integer, String>>() {
            @Override
            public int compare(Pair<Integer, String> pair1, Pair<Integer, String> pair2) {
                return pair1.x.compareTo(pair2.x) * -1;
            }
        });

        for (Pair<Integer, String> item : rankList) {
            Text airport = new Text(item.y);
            IntWritable count = new IntWritable(item.x);
            context.write(airport, count);
        }
    }
}
