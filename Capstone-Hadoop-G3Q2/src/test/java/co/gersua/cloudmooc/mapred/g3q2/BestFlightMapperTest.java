package co.gersua.cloudmooc.mapred.g3q2;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class BestFlightMapperTest {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    @Test
    public void testMap() throws Exception {

        BufferedReader br = new BufferedReader(new FileReader(("/Users/gersua/Projects/cloud-mooc-capstone/" +
                "Capstone-Hadoop-G3Q2/src/test/resources/co/gersua/cloudmooc/mapred/g3q2/test.txt")));

        String lineRead;
        while ((lineRead = br.readLine()) != null){

            String line = lineRead;
            String[] data = line.split("\\s");

            if (data.length >= 5) {
                String originAirport = data[0].replaceAll("\"", "");
                String destAirport = data[1].replaceAll("\"", "");
                String flightDate = data[2];
                String departureTime = data[3].replaceAll("\"", "");
                String arrivalDelay = data[4];

                try {
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(DATE_FORMAT.parse(flightDate));
                    calendar.set(Calendar.HOUR, 0);
                    calendar.set(Calendar.MINUTE, 0);
                    calendar.set(Calendar.SECOND, 0);
                    // Increase 2 days for origin keys
                    calendar.roll(Calendar.DAY_OF_YEAR, 2);
                    String flightFutureDate = DATE_FORMAT.format(calendar.getTime());

                    int departureTimeInt = Integer.valueOf(departureTime);

                    // Origin Key/Value
                    String originKey = String.format("%s:%s", originAirport, flightFutureDate);
                    String originValue =
                            String.format("%s\t%s\t%s\t%s\t%s", destAirport, flightDate, departureTime, arrivalDelay, "ORG");

                    // Destination Key/Value
                    String destKey = String.format("%s:%s", destAirport, flightDate);
                    String destValue =
                            String.format("%s\t%s\t%s\t%s\t%s", originAirport, flightDate, departureTime, arrivalDelay, "DST");

                    if (departureTimeInt == 1200) {
                        // Create both keys
//                        context.write(new Text(originKey), new Text(originValue));
//                        context.write(new Text(destKey), new Text(destValue));
                        System.out.printf("ORG Key: %s\tValue: %s\n", originKey, originValue);
                        System.out.printf("DST Key: %s\tValue: %s\n", destKey, destValue);
                    } else if (departureTimeInt <= 1200) {
                        // Create ORG
                        System.out.printf("ORG Key: %s\tValue: %s\n", originKey, originValue);
//                        context.write(new Text(originKey), new Text(originValue));
                    } else {
                        // Create DST
                        System.out.printf("DST Key: %s\tValue: %s\n", destKey, destValue);
//                        context.write(new Text(destKey), new Text(destValue));
                    }
                } catch (ParseException ex) {
                    System.out.println("Problem was found: " + ex);
                }
            }






        }

        br.close();





    }
}