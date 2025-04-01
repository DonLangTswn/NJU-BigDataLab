package com.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: <INPUT> <OUTPUT> <STOPWORDS>");
            return -1;
        }
        String input_path = args[0];
        String output_path = args[1];
        String stopwords_path = args[2];
        String temp_path = "user/221220070stu/temp-" + System.currentTimeMillis();

        Job index_job = Job.getInstance(super.getConf(), "InvertedIndex");

        // Add stopwords-file to cache
        index_job.addCacheFile(new Path(stopwords_path).toUri());

        index_job.setJarByClass(InvertedIndex.class);           // Main class
        index_job.setMapperClass(InvertedIndexMapper.class);    // Mapper class
        index_job.setReducerClass(InvertedIndexReducer.class);  // Reducer class
        // Index-job's Mapper's output key-value type
        index_job.setMapOutputKeyClass(Text.class);
        index_job.setMapOutputValueClass(Text.class);
        // Index-job's output key-value type
        index_job.setOutputKeyClass(Text.class);
        index_job.setOutputValueClass(Text.class);
        // input path
        index_job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(index_job, new Path(input_path));
        // output path (tmp)
        index_job.setOutputFormatClass(SequenceFileOutputFormat.class); // 中间数据：使用二进制文件格式
        SequenceFileOutputFormat.setOutputPath(index_job, new Path(temp_path));

        if (!index_job.waitForCompletion(true)) {
            return 1;
        }
        Job sort_job = Job.getInstance(super.getConf(), "SortByAvgAppear");
        sort_job.setJarByClass(InvertedIndex.class);    // Main class
        sort_job.setMapperClass(SortMapper.class);      // Mapper class
        sort_job.setReducerClass(SortReducer.class);    // Reducer class
        // Sort-job's Mapper's output key-value type
        sort_job.setMapOutputKeyClass(DoubleWritable.class);
        sort_job.setMapOutputValueClass(Text.class);
        // 排序：降序 (用升序 + 对原值取负实现)
        sort_job.setSortComparatorClass(DoubleWritable.Comparator.class);
        // Sort-job's output key-value type
        sort_job.setOutputKeyClass(Text.class);
        sort_job.setOutputValueClass(Text.class);
        // input path (tmp)
        sort_job.setInputFormatClass(SequenceFileInputFormat.class);    // 中间数据：使用二进制文件格式
        SequenceFileInputFormat.addInputPath(sort_job, new Path(temp_path));
        // output path
        sort_job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(sort_job, new Path(output_path));

        if (sort_job.waitForCompletion(true)) {
            FileSystem.get(sort_job.getConfiguration()).delete(new Path(temp_path), true);
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int res = ToolRunner.run(configuration, new InvertedIndex(), args);
        System.exit(res);
    }
}
