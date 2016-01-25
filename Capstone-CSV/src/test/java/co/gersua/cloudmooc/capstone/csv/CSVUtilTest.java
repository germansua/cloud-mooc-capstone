package co.gersua.cloudmooc.capstone.csv;

import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CSVUtilTest{

    @Test
    public void uniqueHeaderSetTest() throws Exception {
        File location = new File(getClass().getResource("test1.csv").toURI()).getParentFile();
        assertNotNull(location);

        Map<String, Integer> headersCount = CSVUtil.headersCount(location);
        assertNotEquals(headersCount.size(), 0);

        Map<String, Integer> expectedMap = new HashMap<>();
        expectedMap.put("header1", 2);
        expectedMap.put("header2", 2);
        expectedMap.put("header3", 2);
        expectedMap.put("header4", 2);
        expectedMap.put("header5", 1);

        assertTrue(headersCount.equals(expectedMap));
    }
}