package co.gersua.cloudmooc.mapred.g2q1q2.all;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class Top10CarriersOnTimeReducer extends Reducer<NullWritable, TextArrayWritable, Text, DoubleWritable> {

    @Override
    public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context)
            throws IOException, InterruptedException {

        Map<String, TreeSet<Pair<Double, String>>> allResults = new HashMap<String, TreeSet<Pair<Double, String>>>();
        InverseComparator inverseComparator = new InverseComparator();

        for (TextArrayWritable value : values) {
            Text[] pairs = (Text[]) value.toArray();
            String airportCarrier = pairs[0].toString();
            Double departureDelay = Double.valueOf(pairs[1].toString());

            int separatorIndex = airportCarrier.indexOf(":");
            String airport = airportCarrier.substring(0, separatorIndex);

            TreeSet<Pair<Double, String>> resultValues = allResults.get(airport);
            if (resultValues == null) {
                resultValues = new TreeSet<Pair<Double, String>>();
                allResults.put(airport, resultValues);
            }

            resultValues.add(new Pair<Double, String>(departureDelay, airportCarrier));
            if (resultValues.size() > 10) {
                resultValues.remove(resultValues.first());
            }
        }

        for (Map.Entry<String, TreeSet<Pair<Double, String>>> entry : allResults.entrySet()) {
            String currentKey = entry.getKey();
            List<Pair<Double, String>> currentValue = new ArrayList<Pair<Double, String>>();
            currentValue.addAll(entry.getValue());
            Collections.sort(currentValue, inverseComparator);

            // Write in context
            for (Pair<Double, String> item : currentValue) {
                Text airport = new Text(item.y);
                DoubleWritable totalDelay = new DoubleWritable(item.x);
                context.write(airport, totalDelay);
            }
        }
    }
}
