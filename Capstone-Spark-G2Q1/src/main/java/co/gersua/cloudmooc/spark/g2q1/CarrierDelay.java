package co.gersua.cloudmooc.spark.g2q1;

import java.io.Serializable;

public class CarrierDelay implements Serializable, Comparable<CarrierDelay> {

    private String carrier;
    private double delay;
    private int count;

    public CarrierDelay(String carrier, double delay, int count) {
        this.carrier = carrier;
        this.delay = delay;
        this.count = count;
    }

    public String getCarrier() {
        return carrier;
    }

    public double getDelay() {
        return delay;
    }

    public int getCount() {
        return count;
    }

    @Override
    public int compareTo(CarrierDelay other) {
        if (other == null) {
            return -1;
        }

        int comparison = carrier.compareTo(other.getCarrier());
        if (comparison == 0) {
            double thisAvg = delay / count;
            double otherAvg = other.getDelay() / other.getCount();

            if (thisAvg == otherAvg) {
                comparison = 0;
            } else if (thisAvg < otherAvg) {
                comparison = -1;
            } else {
                comparison = 1;
            }
        }

        return comparison;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CarrierDelay that = (CarrierDelay) o;

        if (Double.compare(that.delay, delay) != 0) return false;
        if (count != that.count) return false;
        return carrier != null ? carrier.equals(that.carrier) : that.carrier == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = carrier != null ? carrier.hashCode() : 0;
        temp = Double.doubleToLongBits(delay);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + count;
        return result;
    }

    @Override
    public String toString() {
        return "CarrierDelay{" +
                "carrier='" + carrier + '\'' +
                ", delay=" + delay +
                ", count=" + count +
                ", average=" + (delay / count) +
                '}';
    }
}
