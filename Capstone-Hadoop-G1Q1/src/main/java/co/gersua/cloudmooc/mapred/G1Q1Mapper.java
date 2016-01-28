package co.gersua.cloudmooc.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class G1Q1Mapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable count = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        Text airport = new Text(value.toString());
        context.write(airport, count);
    }
}
