package co.gersua.cloudmooc.capstone.csv.tx;

import co.gersua.cloudmooc.capstone.csv.tx.CSVTransformer;
import junit.framework.TestCase;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class CSVTransformerTest extends TestCase {

    public void testWordSplitter() throws Exception {
        List<String> expectedWords = Arrays.asList("Hello", "", "\"Complex, World\"", "!", "", "l");
        List<String> actualWords = CSVTransformer.wordSplitter("Hello,,\"Complex, World\",!,,l");
        assertTrue(expectedWords.equals(actualWords));

        expectedWords = Arrays.asList("Hello", "", "World", "!", "", "");
        actualWords = CSVTransformer.wordSplitter("Hello,,World,!,,");
        assertTrue(expectedWords.equals(actualWords));
    }

    public void testTransform() throws Exception {
        File src = new File(getClass().getResource("transformTest.csv").toURI());
        File dst = File.createTempFile(src.getName(), "modified.csv");
        dst.deleteOnExit();
        CSVTransformer.transform(src, dst, "\"Origin\"", "\"Dest\"");
    }

    public void testTransformFiles() throws Exception {
        File src = new File(getClass().getResource("transformTest.csv").toURI());
        File dst = new File(System.getProperty("java.io.tmpdir"));
        CSVTransformer.transformFiles(src.getParentFile(), dst, ".tx.g1.q1", "\"Origin\"", "\"Dest\"");
//        File file = new File("/Users/gersua/Data/data/aviation/airline_ontime/2001/" +
//                "On_Time_On_Time_Performance_2001_1/On_Time_On_Time_Performance_2001_1.csv");
    }
}