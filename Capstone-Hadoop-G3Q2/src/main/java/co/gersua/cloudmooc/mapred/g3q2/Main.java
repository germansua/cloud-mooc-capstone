package co.gersua.cloudmooc.mapred.g3q2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job bestFlightJob = Job.getInstance(conf, "Flights Job");
        bestFlightJob.setJarByClass(Main.class);

        bestFlightJob.setOutputKeyClass(Text.class);
        bestFlightJob.setOutputValueClass(Text.class);

        bestFlightJob.setMapOutputKeyClass(Text.class);
        bestFlightJob.setMapOutputValueClass(Text.class);

        bestFlightJob.setMapperClass(BestFlightMapper.class);
//        bestFlightJob.setCombinerClass(BestFlightReducer.class);
        bestFlightJob.setReducerClass(BestFlightReducer.class);

        FileInputFormat.addInputPath(bestFlightJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(bestFlightJob, new Path(args[1]));

        System.exit(bestFlightJob.waitForCompletion(true) ? 0 : 1);
    }
}
