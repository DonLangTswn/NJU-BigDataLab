package com.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SharedFollowReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> mos;
    int threshold = 0;

    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
        Configuration conf = context.getConfiguration();
        threshold = conf.getInt("common.follow.threshold", 0);
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        mos.close();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // 第一个用户的关注
        List<String> first_follow = new ArrayList<>();
        // 共同关注列表
        StringBuffer shared_follow = new StringBuffer();
        int shared_num = 0;
        for (Text value: values) {
            List<String> followings = Arrays.asList(value.toString().split(" "));
            // userA：先将userA的关注列表全部加入first_follow
            if (first_follow.isEmpty()) {
                first_follow.addAll(followings);
            }
            // userB：遍历first_follow，看是不是被userB也关注
            else {
                for (String shared: first_follow) {
                    if (followings.contains(shared)) {
                        if (shared_follow.length() > 0) {
                            shared_follow.append(" ");
                        }
                        shared_follow.append(shared);
                        shared_num += 1;
                    }
                }
            }
        }
        if (shared_num <= threshold) {
            mos.write("below"+ threshold, key, new Text(shared_follow.toString()));
        }
        else {
            mos.write("above"+ threshold, key, new Text(shared_follow.toString()));
        }
    }
}
