package co.gersua.cloudmooc.mapred.g2q1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Top10CarriersOnTimeReducer extends Reducer<NullWritable, TextArrayWritable, Text, DoubleWritable> {

    private static final int MAX_RESULTS = 10;

    private List<Pair<Double, String>> resultsCMI = new ArrayList<Pair<Double, String>>();
    private List<Pair<Double, String>> resultsBWI = new ArrayList<Pair<Double, String>>();
    private List<Pair<Double, String>> resultsMIA = new ArrayList<Pair<Double, String>>();
    private List<Pair<Double, String>> resultsLAX = new ArrayList<Pair<Double, String>>();
    private List<Pair<Double, String>> resultsIAH = new ArrayList<Pair<Double, String>>();
    private List<Pair<Double, String>> resultsSFO = new ArrayList<Pair<Double, String>>();

    @Override
    public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context)
            throws IOException, InterruptedException {

        for (TextArrayWritable value : values) {
            Text[] pairs = (Text[]) value.toArray();
            String airportCarrier = pairs[0].toString();
            Double departureDelay = Double.valueOf(pairs[1].toString());

            if (airportCarrier.contains("\"CMI\"")) {
                resultsCMI.add(new Pair<Double, String>(departureDelay, airportCarrier));
            } else if (airportCarrier.contains("\"BWI\"")) {
                resultsBWI.add(new Pair<Double, String>(departureDelay, airportCarrier));
            } else if (airportCarrier.contains("\"MIA\"")) {
                resultsMIA.add(new Pair<Double, String>(departureDelay, airportCarrier));
            } else if (airportCarrier.contains("\"LAX\"")) {
                resultsLAX.add(new Pair<Double, String>(departureDelay, airportCarrier));
            } else if (airportCarrier.contains("\"IAH\"")) {
                resultsIAH.add(new Pair<Double, String>(departureDelay, airportCarrier));
            } else if (airportCarrier.contains("\"SFO\"")) {
                resultsSFO.add(new Pair<Double, String>(departureDelay, airportCarrier));
            }
        }

        organizeList(resultsCMI, resultsBWI, resultsMIA, resultsLAX, resultsIAH, resultsSFO);

        List<Pair<Double, String>> resultsCMISublist = resultsCMI.subList(0, resultsCMI.size() > MAX_RESULTS ? MAX_RESULTS : resultsCMI.size());
        List<Pair<Double, String>> resultsBWISublist = resultsBWI.subList(0, resultsBWI.size() > MAX_RESULTS ? MAX_RESULTS : resultsBWI.size());
        List<Pair<Double, String>> resultsMIASublist = resultsMIA.subList(0, resultsMIA.size() > MAX_RESULTS ? MAX_RESULTS : resultsMIA.size());
        List<Pair<Double, String>> resultsLAXSublist = resultsLAX.subList(0, resultsLAX.size() > MAX_RESULTS ? MAX_RESULTS : resultsLAX.size());
        List<Pair<Double, String>> resultsIAHSublist = resultsIAH.subList(0, resultsIAH.size() > MAX_RESULTS ? MAX_RESULTS : resultsIAH.size());
        List<Pair<Double, String>> resultsSFOSublist = resultsSFO.subList(0, resultsSFO.size() > MAX_RESULTS ? MAX_RESULTS : resultsSFO.size());

        writeList(context,
                resultsCMISublist, resultsBWISublist, resultsMIASublist, resultsLAXSublist, resultsIAHSublist, resultsSFOSublist);
    }

    private void organizeList(List<Pair<Double, String>>... lists) {
        InverseComparator inverseComparator = new InverseComparator();
        for (List list : lists) {
            Collections.sort(list, inverseComparator);
        }
    }

    private void writeList(Context context, List<Pair<Double, String>>... lists)
            throws IOException, InterruptedException {

        for (List<Pair<Double, String>> list : lists) {
            for (Pair<Double, String> item : list) {
                Text airport = new Text(item.y);
                DoubleWritable totalDelay = new DoubleWritable(item.x);
                context.write(airport, totalDelay);
            }
        }
    }
}
