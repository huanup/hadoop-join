package com.mine;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhanghuan on 2018/12/11.
 * 员工工资比领导高
 */
public class EmpMoreThanLeader {

    static class MoreMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] item = value.toString().split(",");
            //上司表
            context.write(new Text(item[0]), new Text("M:" + item[5]));
            if(StringUtils.isNotBlank(item[3])){
                //员工对应的上司表
                context.write(new Text(item[3]), new Text("E:" + item[1] + ":" + item[5]));
            }
            //员工和上司 shuffle阶段划分到同一个reducer
        }
    }

    static class MoreReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //员工信息
            Map<String, Long> empMap = new HashMap<>();
            Long salary = 0L;
            for (Text text : values) {
                String[] msg = text.toString().split(":");
                if ("E".equals(msg[0])) {
                    empMap.put(msg[1], Long.parseLong(msg[2]));
                } else {
                    salary = Long.parseLong(msg[1]);
                }
            }

            for (java.util.Map.Entry<String, Long> entry : empMap.entrySet()) {
                if (entry.getValue() > salary) {
                    context.write(new Text(entry.getKey()), new Text("" + entry.getValue()));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("args length must >= 2");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(EmpMoreThanLeader.class);
        job.setJobName("emp more than leader");
        job.setMapperClass(MoreMap.class);
        job.setReducerClass(MoreReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new Path(Common.getPath(args[0])).toUri());
        for (int i = 0, len = args.length - 1; i < len; i++) {
            FileInputFormat.addInputPath(job, new Path(Common.getPath(args[i])));
        }
        Common.delete(args[args.length - 1], conf);
        FileOutputFormat.setOutputPath(job, new Path(Common.getPath(args[args.length - 1])));
        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }

}
