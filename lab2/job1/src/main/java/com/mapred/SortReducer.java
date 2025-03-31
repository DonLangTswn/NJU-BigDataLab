package com.mapred;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Reducer<DoubleWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Text word = new Text(), data = new Text();
        for (Text value: values) {
            String[] lineVal = value.toString().split("/t");
            word.set(lineVal[0]);
            data.set(lineVal[1]);
            context.write(word, data);
        }
    }
}
