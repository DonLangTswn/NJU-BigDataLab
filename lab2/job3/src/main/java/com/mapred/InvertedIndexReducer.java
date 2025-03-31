package com.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        StringBuilder appearance = new StringBuilder();
        // Map：文档名 - 词语出现次数
        Map<String, Integer> docAppCount = new HashMap<>();
        int totalAppear = 0;
        for (Text value: values) {
            String docName = value.toString();
            docAppCount.put(docName, docAppCount.getOrDefault(docName, 0) + 1);
            totalAppear += 1;
        }
        // 根据 Map 构造输出字符串
        docAppCount.forEach((docName, appearCnt) -> {
            if (appearance.length() > 0) {
                appearance.append("; ");
            }
            appearance.append(docName).append(":").append(appearCnt);
        });
        // 平均提及 = 总提及次数 / 提及文档数
        double avgAppear = (double) totalAppear / docAppCount.size();
        appearance.insert(0, String.format("%.2f, ", avgAppear));

        context.write(new Text("[" + key.toString() + "]"), new Text(appearance.toString()));
    }
}
