package co.gersua.cloudmooc.mapred.g2q3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10CarriersOnTimeReducer extends Reducer<NullWritable, TextArrayWritable, Text, DoubleWritable> {

    private static final int MAX_RESULTS = 10;
    private static final List<String> allowedRoutes =
            Arrays.asList("\"CMI\":\"ORD\"", "\"IND\":\"CMH\"", "\"DFW\":\"IAH\"", "\"LAX\":\"SFO\"", "\"JFK\":\"LAX\"", "\"ATL\":\"PHX\"");

    private List<Pair<Double, String>> CMI_ORD = new ArrayList<Pair<Double, String>>();
    private List<Pair<Double, String>> IND_CMH = new ArrayList<Pair<Double, String>>();
    private List<Pair<Double, String>> DFW_IAH = new ArrayList<Pair<Double, String>>();
    private List<Pair<Double, String>> LAX_SFO = new ArrayList<Pair<Double, String>>();
    private List<Pair<Double, String>> JFK_LAX = new ArrayList<Pair<Double, String>>();
    private List<Pair<Double, String>> ATL_PHX = new ArrayList<Pair<Double, String>>();

    @Override
    public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context)
            throws IOException, InterruptedException {

        for (TextArrayWritable value : values) {
            Text[] pairs = (Text[]) value.toArray();
            String sourceDestCarrier = pairs[0].toString();
            Double arrivalPerformance = Double.valueOf(pairs[1].toString());

            if (sourceDestCarrier.contains(allowedRoutes.get(0))) {
                CMI_ORD.add(new Pair<Double, String>(arrivalPerformance, sourceDestCarrier));
            } else if (sourceDestCarrier.contains(allowedRoutes.get(1))) {
                IND_CMH.add(new Pair<Double, String>(arrivalPerformance, sourceDestCarrier));
            } else if (sourceDestCarrier.contains(allowedRoutes.get(2))) {
                DFW_IAH.add(new Pair<Double, String>(arrivalPerformance, sourceDestCarrier));
            } else if (sourceDestCarrier.contains(allowedRoutes.get(3))) {
                LAX_SFO.add(new Pair<Double, String>(arrivalPerformance, sourceDestCarrier));
            } else if (sourceDestCarrier.contains(allowedRoutes.get(4))) {
                JFK_LAX.add(new Pair<Double, String>(arrivalPerformance, sourceDestCarrier));
            } else if (sourceDestCarrier.contains(allowedRoutes.get(5))) {
                ATL_PHX.add(new Pair<Double, String>(arrivalPerformance, sourceDestCarrier));
            }
        }

        organizeList(CMI_ORD, IND_CMH, DFW_IAH, LAX_SFO, JFK_LAX, ATL_PHX);

        List<Pair<Double, String>> resultsCMISublist = CMI_ORD
                .subList(0, CMI_ORD.size() > MAX_RESULTS ? MAX_RESULTS : CMI_ORD.size());
        List<Pair<Double, String>> resultsBWISublist = IND_CMH
                .subList(0, IND_CMH.size() > MAX_RESULTS ? MAX_RESULTS : IND_CMH.size());
        List<Pair<Double, String>> resultsMIASublist = DFW_IAH
                .subList(0, DFW_IAH.size() > MAX_RESULTS ? MAX_RESULTS : DFW_IAH.size());
        List<Pair<Double, String>> resultsLAXSublist = LAX_SFO
                .subList(0, LAX_SFO.size() > MAX_RESULTS ? MAX_RESULTS : LAX_SFO.size());
        List<Pair<Double, String>> resultsIAHSublist = JFK_LAX
                .subList(0, JFK_LAX.size() > MAX_RESULTS ? MAX_RESULTS : JFK_LAX.size());
        List<Pair<Double, String>> resultsSFOSublist = ATL_PHX
                .subList(0, ATL_PHX.size() > MAX_RESULTS ? MAX_RESULTS : ATL_PHX.size());

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
                Text sourceDestCarrier = new Text(item.y);
                DoubleWritable arrivalPerformance = new DoubleWritable(item.x);
                context.write(sourceDestCarrier, arrivalPerformance);
            }
        }
    }
}
