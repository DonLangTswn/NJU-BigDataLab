package com.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SharedFollow extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String input_path = args[1];
        String output_path = args[2];
        String mutuals_path = args[3];
        Job shared_job = Job.getInstance(super.getConf(), "Shared Following");
        // 添加 mutuals.txt 作为缓存文件
        shared_job.addCacheFile(new Path(mutuals_path).toUri());
        // set Main class
        shared_job.setJarByClass(SharedFollow.class);
        shared_job.setMapperClass(SharedFollowMapper.class);
        shared_job.setReducerClass(SharedFollowReducer.class);
        // Mapper's output key-value type
        shared_job.setMapOutputKeyClass(Text.class);
        shared_job.setMapOutputValueClass(Text.class);
        // output key-value type
        shared_job.setOutputKeyClass(Text.class);
        shared_job.setOutputValueClass(Text.class);
        // input path 
        shared_job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(shared_job, new Path(input_path));
        // output path
        shared_job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(shared_job, new Path(output_path));
        shared_job.getConfiguration().set(
            "mapreduce.output.textoutputformat.separator", ": ");
        // 多文件输出
        MultipleOutputs.addNamedOutput(shared_job, "below"+ args[0], TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(shared_job, "above"+ args[0], TextOutputFormat.class, Text.class, Text.class);
        
        if (shared_job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: <THRESHOLD> <INPUT> <OUTPUT> <MUTUALS>!!!");
            System.exit(-1);
        }
        Configuration configuration = new Configuration();
        configuration.setInt("common.follow.threshold", Integer.parseInt(args[0]));
        int res = ToolRunner.run(configuration, new SharedFollow(), args);
        System.exit(res);
    }
}
