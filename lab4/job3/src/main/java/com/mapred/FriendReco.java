package com.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendReco extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String input_path = args[0];
        String output_path = args[1];
        String mutuals_path = args[2];
        Job job = Job.getInstance(super.getConf(), "Friend Recommandation");
        // 添加 mutuals.txt 作为缓存文件
        job.addCacheFile(new Path(mutuals_path).toUri());
        // set Main class
        job.setJarByClass(FriendReco.class);
        job.setMapperClass(FriendRecoMapper.class);
        job.setReducerClass(FriendRecoReducer.class);
        // Mapper's output key-value type
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // output key-value type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // input path 
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(input_path));
        // output path
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output_path));
        // job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
        
        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: <INPUT> <OUTPUT> <MUTUALS>!!!");
            System.exit(-1);
        }
        Configuration configuration = new Configuration();
        int res = ToolRunner.run(configuration, new FriendReco(), args);
        System.exit(res);
    }
}
