package com.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendRecoReducer extends Reducer<Text, Text, Text, Text> {
    /*
     * @param: key-用户，values-推送给用户的“推荐用户”们
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // 构建`推荐用户 - 推荐次数`的 Map
        Map<String, Integer> recommand_list = new HashMap<>();
        for (Text value: values) {
            String recommanded = value.toString();
            recommand_list.put(recommanded, 
                recommand_list.getOrDefault(recommanded, 0) + 1);
        }
        // 对 Map 进行降序排序
        List<Map.Entry<String, Integer>> sorted_reclist = new ArrayList<>(recommand_list.entrySet());
        sorted_reclist.sort((e1, e2) -> {
            int cmp = Integer.compare(e2.getValue(), e1.getValue());    // 实现降序
            if (cmp == 0) {
                return e1.getKey().compareTo(e2.getKey());
            }
            return cmp;
        });
        // 取前五个推荐好友
        int count = 0;
        StringBuffer top5_recommands = new StringBuffer();
        for (Map.Entry<String, Integer> entry: sorted_reclist) {
            if (count >= 5) {
                break;
            } if (count > 0) {
                top5_recommands.append(",");
            }
            top5_recommands.append(entry.getKey());
            count += 1;
        }
        context.write(key, new Text(top5_recommands.toString()));
    }
}
