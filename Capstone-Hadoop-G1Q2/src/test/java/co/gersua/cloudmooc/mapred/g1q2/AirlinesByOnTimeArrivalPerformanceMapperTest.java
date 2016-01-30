package co.gersua.cloudmooc.mapred.g1q2;

import org.junit.Test;

import static org.junit.Assert.*;

public class AirlinesByOnTimeArrivalPerformanceMapperTest {

    @Test
    public void testMap() throws Exception {
        String line = "01010\t10.5";
        String[] data = line.split("\\s");
        assertEquals(2, data.length);
        assertEquals("01010", data[0]);
        assertEquals("10.5", data[1]);
    }
}