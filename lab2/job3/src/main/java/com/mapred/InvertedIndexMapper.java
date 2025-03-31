package com.mapred;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

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

        Set<String> stopwords = new HashSet<>();    // stopwords-list
        // Get stopwords-file
        URI[] stopwordUris = context.getCacheFiles();
        if (stopwordUris != null && stopwordUris.length > 0) {
            // Open stopwords-file with "try"
            try (BufferedReader reader = new BufferedReader(new FileReader(stopwordUris[0].getPath()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();     // clear the space
                    if (!line.isEmpty()) {
                        stopwords.add(line);
                    }
                }
            }
        }

        Text text_word = new Text();
        Text text_fileName = new Text(fileName);
        String[] words = value.toString().split(" ");
        for (String word: words) {
            if (stopwords.contains(word)) {
                continue;
            }
            text_word.set(word);
            // Mapper输出：词语 - 文件名
            context.write(text_word, text_fileName);
        }
    }
}
