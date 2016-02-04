package co.gersua.cloudmooc.mapred.g3q2;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BestFlightReducer extends Reducer<Text, Text, Text, Text> {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        List<FlightInfo> origins = new ArrayList<FlightInfo>();
        List<FlightInfo> destinations = new ArrayList<FlightInfo>();

        for (Text value : values) {
            FlightInfo flightInfo;
            try {
                flightInfo = new FlightInfo(key.toString(), value.toString());
            }
            catch (FlightException ex) {
                continue;
            }

            if (flightInfo.getType().equals("ORG")) {
                if (flightInfo.getDepartureTime() >= 1200) {
                    origins.add(flightInfo);
                }
            } else {
                if (flightInfo.getDepartureTime() <= 1200) {
                    destinations.add(flightInfo);
                }
            }
        }

        if (origins.isEmpty() || destinations.isEmpty()) {
            return;
        }

        context.write(new Text(String.valueOf(origins.size())), new Text(String.valueOf(destinations.size())));

        //        TreeSet<FlightResult> resultSet = new TreeSet<FlightResult>();

        Text keyText = new Text();
        Text valueText = new Text();

        for (FlightInfo dest : destinations) {
            for (FlightInfo org : origins) {

                Calendar calendarOrg = Calendar.getInstance();
                calendarOrg.setTime(org.getFlightDate());

                Calendar calendarDest = Calendar.getInstance();
                calendarDest.setTime(dest.getFlightDate());

                calendarOrg.roll(Calendar.DAY_OF_YEAR, 2);
                if (calendarOrg.compareTo(calendarDest) == 0) {

                    String keyOuput = String
                            .format("%s->%s->%s", org.getKeyAirport(), dest.getKeyAirport(), dest.getValueAirport());

                    String firstDateString = DATE_FORMAT.format(org.getFlightDate());
                    String secondDateString = DATE_FORMAT.format(dest.getFlightDate());
                    long total = org.getArrivalDelay() + dest.getArrivalDelay();
                    String valueOuput = String.format("%s%s%d", firstDateString, secondDateString, total);

                    keyText.set(keyOuput);
                    valueText.set(valueOuput);
                    context.write(keyText, valueText);
                }
            }
        }


        //
        //        for (FlightResult flightResult : resultSet) {
        //            String keyOuput = String.format("%s:%s:%s",
        //                    flightResult.getOriginAirport(),
        //                    flightResult.getStopAirport(),
        //                    flightResult.getDestinationAirport());
        //
        //            String valueOuput = flightResult.toString();
        //            context.write(new Text(keyOuput), new Text(valueOuput));
        //        }
    }
}
