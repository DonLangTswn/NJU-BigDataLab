package com.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FollowerReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        StringBuilder followers = new StringBuilder();
        for (Text value: values) {
            String follower = value.toString();
            if (followers.length() > 0) {
                followers.append(" ");
            }
            followers.append(follower);
        }
        // 输出：博主-粉丝列表
        context.write(key, new Text(followers.toString()));
    }
}
