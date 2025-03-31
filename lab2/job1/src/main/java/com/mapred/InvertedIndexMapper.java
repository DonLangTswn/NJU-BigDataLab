package com.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // 获取输入文件、文件名
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName().toString();

        Text text_word = new Text();
        Text text_fileName = new Text(fileName);

        String[] words = value.toString().split(" ");
        for (String word: words) {
            text_word.set(word);
            // Mapper输出：词语 - 文件名
            context.write(text_word, text_fileName);
        }
    }
}
