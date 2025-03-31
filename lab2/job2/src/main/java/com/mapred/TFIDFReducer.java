package com.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        // Map：记录词语出现在哪几个文档中, 各出现了几次
        Map<String, Integer> tfMap = new HashMap<>();
        for (Text value: values) {
            String fileName = value.toString();
            // Appear-time in current file, default by zero
            int appearTime = tfMap.getOrDefault(fileName, 0);
            tfMap.put(fileName, appearTime + 1);    // increase by 1
        }
        double ratio = (double) 7 / (tfMap.size() + 1);  // 文档数 / 词语出现的文档数 + 1
        double idf = Math.log(ratio) / Math.log(2);    // IDF
        
        Text tfIdfKey = new Text();
        DoubleWritable tfIdfValue = new DoubleWritable();
        for (Map.Entry<String, Integer> entry: tfMap.entrySet()) {
            String fileName = entry.getKey();
            int tf = entry.getValue();          // TF
            double tfIdf = (double) tf * idf;   // TF-IDF = TF*IDF
            // FORMAT: 作品名称, 词语, 该词语的TF-IDF
            tfIdfKey.set(fileName + ", " + key.toString() + ",");
            tfIdfValue.set(tfIdf);

            context.write(tfIdfKey, tfIdfValue);
        }
    }
}
