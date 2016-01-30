package co.gersua.cloudmooc.mapred.g1q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
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

    @Test
    public void mapper2Test() throws Exception {

        String[] data = "19977\t0.00".toString().split("\\s");
        String airlineId = data[0];
        double arrDelayMinutes = Double.valueOf(data[1]);

        System.out.println(String.format("Size: %d, data[0]: %s, data[1]: %s", data.length, data[0], data[1]));
    }
}