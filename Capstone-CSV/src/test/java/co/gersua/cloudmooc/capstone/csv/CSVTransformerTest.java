package co.gersua.cloudmooc.capstone.csv;

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
        File src = new File("/Users/gersua/Desktop/On_Time_On_Time_Performance_1988_1.csv");
        File dst = new File("/Users/gersua/Desktop/", src.getName().concat(".modified.csv"));

        CSVTransformer.transform(src, dst, "\"Origin\"", "\"Dest\"");
    }

    public void testTransformFiles() throws Exception {
        CSVTransformer.transformFiles(new File("/Users/gersua/Desktop"), new File("/Users/gersua/Desktop/Test/common"), ".tx.g1.q1", "\"Origin\"", "\"Dest\"");
    }
}