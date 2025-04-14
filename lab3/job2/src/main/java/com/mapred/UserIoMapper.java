package com.mapred;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserIoMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        String op_name = fields[4];
        String op_count = fields[8];
        String user_namespace = fields[5];
        if (op_name.equals("2")) {
            long op_count_val = Long.valueOf(op_count);
            context.write(new Text(user_namespace), new LongWritable(op_count_val));
        }
    }
}
