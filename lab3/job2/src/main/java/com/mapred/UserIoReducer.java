package com.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserIoReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values,
            Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long sum_count = 0;
        // 对同一个user_namespace对应的op_count进行累加
        for (LongWritable value: values) {
            sum_count += value.get();
        }
        context.write(key, new LongWritable(sum_count));
    }
}
