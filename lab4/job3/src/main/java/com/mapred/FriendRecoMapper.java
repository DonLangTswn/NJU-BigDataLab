package com.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendRecoMapper extends Mapper<LongWritable, Text, Text, Text> {
    // 互关列表Map
    private HashMap<String, List<String>> mutuals;
    
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        mutuals = new HashMap<>();
        // 读取附加文件：互关列表
        URI[] mutualsUris = context.getCacheFiles();
        if (mutualsUris != null && mutualsUris.length > 0) {
            Path mutualsPath = new Path(mutualsUris[0].toString());
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(mutualsPath)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // 分解互关列表的每行数据，将相互的互关关系都加入Map
                    String[] mutual_pair = line.toString().split("-");
                    String userA = mutual_pair[0];
                    String userB = mutual_pair[1];
                    mutuals.computeIfAbsent(userA, f -> new ArrayList<String>()).add(userB);
                    mutuals.computeIfAbsent(userB, f -> new ArrayList<String>()).add(userA);
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(": ");
        if (data.length < 2) {
            return;
        }
        String user = data[0];
        Text user_text = new Text(user);
        Text recommand = new Text();
        List<String> followings = Arrays.asList(data[1].split(" "));
        // 找 user 的互关好友 friendA
        for (String friendA: mutuals.getOrDefault(user, new ArrayList<>())) {
            // 再找 friendA 的互关好友 friendB
            for (String friendB: mutuals.get(friendA)) {
                // 如果 user 没有关注 friendB
                if (!user.equals(friendB) && !followings.contains(friendB)) {
                    recommand.set(friendB);
                    context.write(user_text, recommand);    // 向 user 推荐 friendB
                }
            }
        }
    }
}
