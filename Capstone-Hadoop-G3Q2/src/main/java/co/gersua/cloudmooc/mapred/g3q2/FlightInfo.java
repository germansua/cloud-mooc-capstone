package co.gersua.cloudmooc.mapred.g3q2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class FlightInfo {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private String keyAirport;
    private String valueAirport;
    private Date flightDate;
    private int departureTime;
    private long arrivalDelay;
    private String type;

    public FlightInfo(String keyAirport, String values) throws FlightException {

        int dotIndex = keyAirport.indexOf(":");
        this.keyAirport = keyAirport.substring(0, dotIndex);

        String[] data = values.split("\\s");
        valueAirport = data[0];

        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(DATE_FORMAT.parse(data[1]));
            calendar.set(Calendar.HOUR, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            flightDate = calendar.getTime();
        } catch (ParseException ex) {
            System.out.println("**** PROBLEM PARSING DATE *** : " + ex);
            throw new FlightException(ex);
        }

        departureTime = Integer.valueOf(data[2].replaceAll("\"",""));
        arrivalDelay = Double.valueOf(data[3]).longValue();
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

    public long getArrivalDelay() {
        return arrivalDelay;
    }

    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return "FlightInfo{" +
                "keyAirport='" + keyAirport + '\'' +
                ", valueAirport='" + valueAirport + '\'' +
                ", flightDate=" + flightDate +
                ", departureTime=" + departureTime +
                ", arrivalDelay=" + arrivalDelay +
                ", type='" + type + '\'' +
                '}';
    }
}
