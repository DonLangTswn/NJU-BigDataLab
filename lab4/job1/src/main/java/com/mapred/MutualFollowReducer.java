package com.mapred;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MutualFollowReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        HashSet<String> user_followers = new HashSet<>();   // 用户的粉丝列表
        HashSet<String> user_followings = new HashSet<>();  // 用户的关注列表
        String user = key.toString();
        Text friend = new Text();
        for (Text value: values) {
            String[] data = value.toString().split("/");
            if (data[0].equals("follower"))
                user_followers.add(data[1]);
            else
                user_followings.add(data[1]);
        }
        // 是否互关：遍历粉丝列表，检查是否在关注列表中
        for (String follower: user_followers) {
            if (user.compareTo(follower) < 0 && user_followings.contains(follower)) {
                friend.set(follower);
                context.write(key, friend);
            }
        }
    }
}
