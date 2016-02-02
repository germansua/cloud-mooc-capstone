package co.gersua.cloudmooc.mapred.g2q1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class Top10CarriersOnTimeReducer extends Reducer<NullWritable, TextArrayWritable, Text, DoubleWritable> {

    // private TreeSet<Pair<Double, String>> topCounter = new TreeSet<Pair<Double, String>>();
    private List<Pair<Double, String>> resultsAsList = new ArrayList<Pair<Double, String>>();

    @Override
    public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context)
            throws IOException, InterruptedException {

        for (TextArrayWritable value : values) {
            Text[] pairs = (Text[]) value.toArray();
            String airportCarrier = pairs[0].toString();
            Double departureDelay = Double.valueOf(pairs[1].toString());
            resultsAsList.add(new Pair<Double, String>(departureDelay, airportCarrier));
        }

        Collections.sort(resultsAsList,
                new Comparator<Pair<Double, String>>() {
                    @Override
                    public int compare(Pair<Double, String> pair1, Pair<Double, String> pair2) {
                        return pair1.compareTo(pair2) * -1;
                    }
                });

        for (Pair<Double, String> item : resultsAsList) {
            Text airport = new Text(item.y);
            DoubleWritable totalDelay = new DoubleWritable(item.x);
            context.write(airport, totalDelay);
        }
    }
}
