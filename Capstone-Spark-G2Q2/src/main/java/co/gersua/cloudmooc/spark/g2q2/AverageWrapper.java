package co.gersua.cloudmooc.spark.g2q2;

import java.io.Serializable;

public class AverageWrapper implements Serializable, Comparable<AverageWrapper> {

    private double value;
    private int count;

    public AverageWrapper(double value, int count) {
        this.value = value;
        this.count = count;
    }

    public double getValue() {
        return value;
    }

    public int getCount() {
        return count;
    }

    @Override
    public int compareTo(AverageWrapper other) {
        if (other == null) {
            return -1;
        }

        return Double.valueOf(value).compareTo(other.getValue());
    }
}
