package co.gersua.cloudmooc.mapred.g2q1;

import java.util.Comparator;

public class InverseComparator implements Comparator<Pair<Double, String>> {

    @Override
    public int compare(Pair<Double, String> pair1, Pair<Double, String> pair2) {
        return pair1.compareTo(pair2) * -1;
    }
}
