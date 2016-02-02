package co.gersua.cloudmooc.mapred.g2q1;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MainTest {

    @Test
    public void test() {

        List<Integer> list = Arrays.asList(1, 2,  3, 4, 5, 6, 7, 8);
        int toIndex = list.size() > 10 ? 10 : list.size();

        List<Integer> integers = list.subList(0, toIndex);

        System.out.println(integers);
    }
}