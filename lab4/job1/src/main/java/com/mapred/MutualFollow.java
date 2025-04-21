package com.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MutualFollow extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: <INPUT> <OUTPUT>!!!");
            return -1;
        }
        String input_path = args[0];
        String output_path = args[1];
        String follower_path = "temp-" + System.currentTimeMillis();

        Job follower_job = Job.getInstance(super.getConf(), "Get Followers");
        follower_job.setJarByClass(MutualFollow.class);
        follower_job.setMapperClass(FollowerMapper.class);
        follower_job.setReducerClass(FollowerReducer.class);
        // follower-job's Mapper's output key-value type
        follower_job.setMapOutputKeyClass(Text.class);
        follower_job.setMapOutputValueClass(Text.class);
        // follower-job's output key-value type
        follower_job.setOutputKeyClass(Text.class);
        follower_job.setOutputValueClass(Text.class);
        // input path
        follower_job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(follower_job, new Path(input_path));
        // output path (tmp)
        follower_job.setOutputFormatClass(TextOutputFormat.class); // 中间数据：仍使用文本文件，与input格式一致
        follower_job.getConfiguration().set(
            "mapreduce.output.textoutputformat.separator", ": ");   // 设置分隔符与input格式一致
        TextOutputFormat.setOutputPath(follower_job, new Path(follower_path));

        if (!follower_job.waitForCompletion(true)) {
            return 1;
        }
        Job mutuals_job = Job.getInstance(super.getConf(), "Mutual-Follow");
        mutuals_job.setJarByClass(MutualFollow.class);
        mutuals_job.setMapperClass(MutualFollowMapper.class);
        mutuals_job.setReducerClass(MutualFollowReducer.class);
        // Mutuals-job's Mapper's output key-value type
        mutuals_job.setMapOutputKeyClass(Text.class);
        mutuals_job.setMapOutputValueClass(Text.class);
        // Mutuals-job's output key-value type
        mutuals_job.setOutputKeyClass(Text.class);
        mutuals_job.setOutputValueClass(Text.class);
        // input path (tmp)
        mutuals_job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(mutuals_job, new Path(input_path));
        TextInputFormat.addInputPath(mutuals_job, new Path(follower_path));
        // output path
        mutuals_job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(mutuals_job, new Path(output_path));
        mutuals_job.getConfiguration().set(
            "mapreduce.output.textoutputformat.separator", "-");   // 设置分隔符为"-"

        if (mutuals_job.waitForCompletion(true)) {
            FileSystem.get(mutuals_job.getConfiguration()).delete(new Path(follower_path), true);
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int res = ToolRunner.run(configuration, new MutualFollow(), args);
        System.exit(res);
    }
}
