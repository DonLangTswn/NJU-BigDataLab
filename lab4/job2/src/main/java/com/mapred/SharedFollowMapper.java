package com.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SharedFollowMapper extends Mapper<LongWritable, Text, Text, Text> {
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
            return; // 没有关注任何人，直接退出
        }
        String user = data[0];
        Text mutual_pair = new Text();
        Text followings = new Text(data[1]);   // 关注列表

        for (String friend: mutuals.get(user)) {
            if (user.compareTo(friend) < 0) {
                mutual_pair.set(user + "-" + friend);
            }else {
                mutual_pair.set(friend + "-" + user);
            }
            // 输出"互关对 : 其中当前user的关注列表"
            context.write(mutual_pair, followings);
        }
    }
}
