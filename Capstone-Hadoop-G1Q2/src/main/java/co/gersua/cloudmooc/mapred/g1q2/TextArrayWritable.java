package co.gersua.cloudmooc.mapred.g1q2;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class TextArrayWritable extends ArrayWritable {

    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(String[] strings) {
        super(Text.class);
        Text[] texts = new Text[strings.length];

        int i = 0;
        for (String s : strings) {
            texts[i++] = new Text(s);
        }
        set(texts);
    }
}
