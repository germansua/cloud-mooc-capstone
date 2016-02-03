package co.gersua.cloudmooc.mapred.g3q2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FlightInfo {

    private String keyAirport;
    private String valueAirport;
    private Date flightDate;
    private int departureTime;
    private Long arrivalDelay;
    private String type;

    public FlightInfo(String keyAirport, String values) throws FlightException {
        this.keyAirport = keyAirport;

        String[] data = values.split("\\s");
        valueAirport = data[0];

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyymmdd");
        try {
            flightDate = dateFormat.parse(data[1]);
        } catch (ParseException ex) {
            System.out.println("**** PROBLEM PARSING DATE *** : " + ex);
            throw new FlightException(ex);
        }

        departureTime = Integer.valueOf(data[2]);
        arrivalDelay = Long.valueOf(data[3]);
        type = data[4];
    }

    public String getKeyAirport() {
        return keyAirport;
    }

    public String getValueAirport() {
        return valueAirport;
    }

    public Date getFlightDate() {
        return flightDate;
    }

    public int getDepartureTime() {
        return departureTime;
    }

    public Long getArrivalDelay() {
        return arrivalDelay;
    }

    public String getType() {
        return type;
    }
}
