package co.gersua.cloudmooc.capstone.csv.tx;

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
        CSVTransformer.transform(src, dst, SeparatorEnum.NEW_LINE, "\"Origin\"", "\"Dest\"");
    }

    public void testTransformNewLineFiles() throws Exception {
        File src = new File(getClass().getResource("transformTest.csv").toURI());
        File dst = new File(System.getProperty("java.io.tmpdir"));
        CSVTransformer.transformFiles(src.getParentFile(), dst,
                ".tx.g1.q1", SeparatorEnum.NEW_LINE, "\"Origin\"", "\"Dest\"");
    }

    public void testTransformTabFiles() throws Exception {
        File src = new File(getClass().getResource("transformTest.csv").toURI());
        File dst = new File(System.getProperty("java.io.tmpdir"));
        CSVTransformer.transformFiles(src.getParentFile(), dst,
                ".tx.g1.q2", SeparatorEnum.TAB, "\"AirlineID\"", "\"ArrDelayMinutes\"");
    }
}