package com.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserIo extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: <INPUT> <OUTPUT>!!!");
            return -1;
        }
        String input_path = args[0];
        String output_path = args[1];
        Job job = Job.getInstance(super.getConf(), "UserIoTimeCount");
        // set classes
        job.setJarByClass(UserIo.class);
        job.setMapperClass(UserIoMapper.class);
        job.setReducerClass(UserIoReducer.class);
        // job Mapper's output key-value type
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // job output key-value type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // input path
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(input_path));
        // output path
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output_path));

        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int res = ToolRunner.run(configuration, new UserIo(), args);
        System.exit(res);
    }
}
