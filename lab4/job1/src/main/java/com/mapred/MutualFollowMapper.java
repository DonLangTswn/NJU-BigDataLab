package com.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MutualFollowMapper extends Mapper<LongWritable, Text, Text, Text> {
    String tag = new String();
    /*
     * Set the tag of the datasource up,
     *   devided by the filename.
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        if (fileName.endsWith(".txt")) {
            // 由".txt"结尾的文件，来源于`用户 - 关注`列表
            tag = "following";
        }
        else tag = "follower";  // 由中间文件读取，来源于`用户 - 粉丝`列表
    }
    
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(": ");
        if (data.length < 2) {
            return; // 没有关注任何人，不可能有互关，直接退出
        }
        Text user = new Text(data[0]);
        Text another_user = new Text();
        String[] others = data[1].split(" ");   // 关注 or 粉丝列表
        for (String other: others) {
            another_user.set(tag + "/" + other);
            // Mapper输出：用户-follower/粉丝 or 用户-following/关注
            context.write(user, another_user);
        }
    }
}
