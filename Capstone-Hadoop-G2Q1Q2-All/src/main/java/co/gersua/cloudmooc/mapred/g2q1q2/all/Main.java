package co.gersua.cloudmooc.mapred.g2q1q2.all;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/w1/tmp");
        fs.delete(tmpPath, true);

        Job airportsByNumberJob = Job.getInstance(conf, "Airport and carriers delay");

        airportsByNumberJob.setOutputKeyClass(Text.class);
        airportsByNumberJob.setOutputValueClass(DoubleWritable.class);

        airportsByNumberJob.setMapperClass(AirportCarrierDelayMapper.class);
        airportsByNumberJob.setReducerClass(AirportCarrierDelayReducer.class);

        FileInputFormat.addInputPath(airportsByNumberJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(airportsByNumberJob, tmpPath);

        airportsByNumberJob.setJarByClass(Main.class);
        airportsByNumberJob.waitForCompletion(true);

        /******************/
        Job topAirportsJob = Job.getInstance(conf, "top 10 carriers by airport");

        topAirportsJob.setOutputKeyClass(Text.class);
        topAirportsJob.setOutputValueClass(DoubleWritable.class);

        topAirportsJob.setMapOutputKeyClass(NullWritable.class);
        topAirportsJob.setMapOutputValueClass(TextArrayWritable.class);
        
        topAirportsJob.setMapperClass(Top10CarriersOnTimeMapper.class);
        topAirportsJob.setReducerClass(Top10CarriersOnTimeReducer.class);
        topAirportsJob.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(topAirportsJob, tmpPath);
        FileOutputFormat.setOutputPath(topAirportsJob, new Path(args[1]));

        topAirportsJob.setInputFormatClass(KeyValueTextInputFormat.class);
        topAirportsJob.setOutputFormatClass(TextOutputFormat.class);

        topAirportsJob.setJarByClass(Main.class);
        System.exit(topAirportsJob.waitForCompletion(true) ? 0 : 1);
    }
}
