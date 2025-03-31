package com.mapred;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Text, Text, DoubleWritable, Text> {
    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, DoubleWritable, Text>.Context context)
            throws IOException, InterruptedException {
        // value 格式："平均次数, doc1:次数; ..."
        String[] partitions = value.toString().split(", ");
        double avgAppear = Double.parseDouble(partitions[0]);

        context.write( new DoubleWritable(-avgAppear),    // 取负实现降序排序
            new Text(key.toString() + "/t" + value.toString()) );
    }
}
