package co.gersua.cloudmooc.spark.g3q2;

import java.io.Serializable;
import java.util.Date;

public class FlightInfo implements Serializable {

    private String origin;
    private String dest;
    private Date flightDate;
    private String flightNum;
    private int crsDepTime;
    private int depTime;
    private double arrDelay;
    private boolean originKey;

    public FlightInfo(String origin, String dest, Date flightDate, String flightNum,
                      int crsDepTime, int depTime, double arrDelay, boolean originKey) {
        this.origin = origin;
        this.dest = dest;
        this.flightDate = flightDate;
        this.flightNum = flightNum;
        this.crsDepTime = crsDepTime;
        this.depTime = depTime;
        this.arrDelay = arrDelay;
        this.originKey = originKey;
    }
}
