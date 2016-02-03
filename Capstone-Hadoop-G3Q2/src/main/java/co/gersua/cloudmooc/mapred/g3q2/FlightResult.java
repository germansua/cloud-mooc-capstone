package co.gersua.cloudmooc.mapred.g3q2;

import java.text.SimpleDateFormat;
import java.util.Date;

public class FlightResult implements Comparable<FlightResult> {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");

    private String originAirport;
    private String stopAirport;
    private String destinationAirport;
    private Date firstDate;
    private Date secondDate;
    private double totalArrivalDelay;

    public FlightResult(String originAirport, String stopAirport, String destinationAirport, Date firstDate,
            Date secondDate, double totalArrivalDelay) {
        this.originAirport = originAirport;
        this.stopAirport = stopAirport;
        this.destinationAirport = destinationAirport;
        this.firstDate = firstDate;
        this.secondDate = secondDate;
        this.totalArrivalDelay = totalArrivalDelay;
    }

    @Override
    public int compareTo(FlightResult otherFlightResult) {

        if (otherFlightResult == null) {
            return -1;
        }

        String current = String.format("%s%s%s%ld", originAirport, stopAirport, destinationAirport, Double.valueOf(totalArrivalDelay).longValue());
        String compare = String.format("%s%s%s%ld", getOriginAirport(), getStopAirport(),
                getDestinationAirport(), getTotalArrivalDelay());

        return current.compareTo(compare);
    }

    @Override
    public String toString() {
        String firstDateString = DATE_FORMAT.format(firstDate);
        String secondDateString = DATE_FORMAT.format(secondDate);
        return String.format("%s\t%s\t%s\t%s\t%s\t%ld",
                originAirport, stopAirport, destinationAirport, firstDateString, secondDateString, Double.valueOf(totalArrivalDelay).longValue());
    }

    public String getOriginAirport() {
        return originAirport;
    }

    public String getStopAirport() {
        return stopAirport;
    }

    public String getDestinationAirport() {
        return destinationAirport;
    }

    public Date getFirstDate() {
        return firstDate;
    }

    public Date getSecondDate() {
        return secondDate;
    }

    public double getTotalArrivalDelay() {
        return totalArrivalDelay;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        FlightResult that = (FlightResult) o;

        if (Double.compare(that.totalArrivalDelay, totalArrivalDelay) != 0)
            return false;
        if (originAirport != null ? !originAirport.equals(that.originAirport) : that.originAirport != null)
            return false;
        if (stopAirport != null ? !stopAirport.equals(that.stopAirport) : that.stopAirport != null)
            return false;
        if (destinationAirport != null ?
                !destinationAirport.equals(that.destinationAirport) :
                that.destinationAirport != null)
            return false;
        if (firstDate != null ? !firstDate.equals(that.firstDate) : that.firstDate != null)
            return false;
        return secondDate != null ? secondDate.equals(that.secondDate) : that.secondDate == null;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = originAirport != null ? originAirport.hashCode() : 0;
        result = 31 * result + (stopAirport != null ? stopAirport.hashCode() : 0);
        result = 31 * result + (destinationAirport != null ? destinationAirport.hashCode() : 0);
        result = 31 * result + (firstDate != null ? firstDate.hashCode() : 0);
        result = 31 * result + (secondDate != null ? secondDate.hashCode() : 0);
        temp = Double.doubleToLongBits(totalArrivalDelay);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
