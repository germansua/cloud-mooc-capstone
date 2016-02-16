package co.gersua.cloudmooc.spark.g3q2;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class MainTest {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    @Test
    public void testApp() throws Exception {



        Date date = DATE_FORMAT.parse("2016-12-31");
        System.out.println("Original: " + date);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);

        calendar.add(Calendar.DATE, 2);
        System.out.println("DATE: " + DATE_FORMAT.format(calendar.getTime()));

        System.out.println(String.valueOf(true));
        System.out.println(String.valueOf(false));
    }
}
