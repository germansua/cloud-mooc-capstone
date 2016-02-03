package co.gersua.cloudmooc.mapred.g3q2;

public class FlightException extends Exception {

    public FlightException() {
        super();
    }

    public FlightException(String message) {
        super(message);
    }

    public FlightException(String message, Throwable cause) {
        super(message, cause);
    }

    public FlightException(Throwable cause) {
        super(cause);
    }

    protected FlightException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
