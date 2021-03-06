package co.gersua.cloudmooc.mapred.g3q2;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BestFlightReducerTest {

    private static final String pattern = "yyyyMMdd";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(pattern);

    @Test
    public void testReduceTimes() throws Exception {
        String time = "0123";
        assertEquals(Long.valueOf(123L), Long.valueOf(time.replaceAll("\"","")));

        String timeQ = "\"1040\"";
        assertEquals(Long.valueOf(1040L), Long.valueOf(timeQ.replaceAll("\"","")));
    }

    @Test
    public void testReduce() throws Exception {
        String dateStringOrg = "20080105";
        String dateStringDst = "20080107";

        Calendar calendarOrg = processDate(dateStringOrg);
        Calendar calendarDst = processDate(dateStringDst);
        calendarOrg.roll(Calendar.DAY_OF_YEAR, 2);
        assertEquals(0, calendarOrg.compareTo(calendarDst));

        dateStringOrg = "20080131";
        dateStringDst = "20080202";

        calendarOrg = processDate(dateStringOrg);
        calendarDst = processDate(dateStringDst);
        calendarOrg.roll(Calendar.DAY_OF_YEAR, 2);
        assertEquals(0, calendarOrg.compareTo(calendarDst));
    }

    private Calendar processDate(String dateString) throws Exception {
        Date date = formatter.parse(dateString);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar;
    }
}