package co.gersua.cloudmooc.spark.g2q1;

import org.junit.Test;

import java.util.TreeSet;

public class MainTest {

    @Test
    public void test() throws Exception {

        CarrierDelay cd1 = new CarrierDelay("DH", 3026.0, 502);
        CarrierDelay cd2 = new CarrierDelay("PI", 131222.0, 16370);
        CarrierDelay cd3 = new CarrierDelay("TW", 1453.0, 218);
        CarrierDelay cd4 = new CarrierDelay("AL", 6398.0, 3147);

        TreeSet<CarrierDelay> ts = new TreeSet<>((c1, c2) ->
                Double.valueOf(c1.getDelay() / c1.getCount()).compareTo(c2.getDelay() / c2.getCount()));
        ts.add(cd1);
        ts.add(cd2);
        ts.add(cd3);
        ts.add(cd4);

        System.out.println(ts);

        ts.pollLast();

        System.out.println(ts);


//        (CMI,CarrierDelay{carrier='MQ', delay=, count=, average=8.016004886988393})
//        (CMI,CarrierDelay{carrier='EV', delay=, count=, average=6.665137614678899})
//        (CMI,CarrierDelay{carrier='US', delay=, count=, average=2.033047346679377})
    }
}
