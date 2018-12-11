package com.mine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhanghuan on 2018/12/10.
 * 求部门工资平均值
 */
public class AvgDeptSalary {

    static class AvgMap extends Mapper<LongWritable, Text, Text, Text>{

        Map<String, String> cache = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] uris = context.getCacheFiles();
            for(URI uri : uris){
                String pathname = uri.toString();
                if(!pathname.contains("dept.txt")) continue;
                FileSystem fs = FileSystem.get(context.getConfiguration());
                try(BufferedReader buffer = new BufferedReader(new InputStreamReader(fs.open(new Path(pathname))))){
                    String deptName = null;
                    while(null != (deptName = buffer.readLine())){
                        cache.put(deptName.split(",")[0], deptName.split(",")[1]);
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] item = value.toString().split(",");
            if(cache.containsKey(item[7])){
                context.write(new Text(item[7]), new Text(item[5]));
            }
        }
    }

    static class AvgReduce extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int sum = 0;
            for(Text value : values){
                sum += Long.parseLong(value.toString());
                count ++;
            }
            context.write(key, new Text("dept memberNum:" + count + " avg salary :" + (count != 0? sum / count : 0 )));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("args length must >= 2");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(AvgDeptSalary.class);
        job.setJobName("avg dept salary");
        job.setMapperClass(AvgMap.class);
        job.setReducerClass(AvgReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new Path(Common.getPath(args[0])).toUri());
        for (int i = 1, len = args.length - 1; i < len; i++) {
            FileInputFormat.addInputPath(job, new Path(Common.getPath(args[i])));
        }
        Common.delete(args[args.length - 1], conf);
        FileOutputFormat.setOutputPath(job, new Path(Common.getPath(args[args.length - 1])));
        System.exit(job.waitForCompletion(true)? 1: 0);
    }
}
