package co.gersua.cloudmooc.spark.g3q2;

import java.io.Serializable;
import java.util.Date;

public class AggregateFlightInfo implements Serializable {

    private FlightInfo firstFlight;
    private FlightInfo secondFlight;

    public AggregateFlightInfo(FlightInfo firstFlight, FlightInfo secondFlight) {
        this.firstFlight = firstFlight;
        this.secondFlight = secondFlight;
    }

    public String originAirport() {
        return firstFlight.getOrigin();
    }

    public String standAirport() {
        return firstFlight.getDest();
    }

    public String destinationAirport() {
        return secondFlight.getDest();
    }

    public double totalArrDelay() {
        return firstFlight.getArrDelay() + secondFlight.getArrDelay();
    }

    public int originCRSDepartTime() {
        return firstFlight.getCrsDepTime();
    }

    public int standCRSDepartTime() {
        return secondFlight.getCrsDepTime();
    }

    public int originDepartTime() {
        return firstFlight.getDepTime();
    }

    public int standDepartTime() {
        return secondFlight.getDepTime();
    }

    public Date originDate() {
        return firstFlight.getFlightDate();
    }

    public Date standDate() {
        return secondFlight.getFlightDate();
    }

    public FlightInfo getFirstFlight() {
        return firstFlight;
    }

    public FlightInfo getSecondFlight() {
        return secondFlight;
    }
}
