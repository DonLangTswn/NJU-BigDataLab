package com.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FollowerMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(": ");
        if (data.length < 2) {
            return; // 没有关注任何人，不是任何人的粉丝，直接退出
        }
        String[] followings = data[1].split(" ");   // 关注列表
        Text user = new Text(data[0]);
        Text user_following = new Text();
        for (String following: followings) {
            // Mapper输出：用户 - 粉丝
            user_following.set(following);
            context.write(user_following, user);
        }
    }
}
