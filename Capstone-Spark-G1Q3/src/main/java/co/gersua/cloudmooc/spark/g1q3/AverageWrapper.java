package co.gersua.cloudmooc.spark.g1q3;

import java.io.Serializable;

public class AverageWrapper implements Serializable {

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
}
