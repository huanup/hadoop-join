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

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhanghuan on 2018/12/10.
 * 求部门工资总和
 */
public class SumDeptSalary {

    static class MyMap extends Mapper<LongWritable, Text, Text, Text> {

        Map<String, String> cache = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] path = context.getCacheFiles();
            String deptName;
            for (URI uri : path) {
                String pathname = uri.toString();
                if (!pathname.endsWith("dept.txt")) continue;
                FileSystem fs = FileSystem.get(context.getConfiguration());
                try (BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(pathname))))) {
                    while (null != (deptName = in.readLine())) {
                        cache.put(deptName.split(",")[0], deptName.split(",")[1]);
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //map join
            String[] item = value.toString().split(",");
            if(cache.containsKey(item[7])){
                context.write(new Text(item[7]), new Text(item[5]));
            }
        }
    }

    static class MyReduce extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(Text value : values){
                sum += Integer.parseInt(value.toString());
            }
            context.write(key, new Text(String.valueOf(sum)));
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        if (args.length < 2) {
            System.err.println("args length must >= 2");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sum dept salary");
        job.setJarByClass(SumDeptSalary.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 1, len = args.length - 1; i < len; i++) {
            FileInputFormat.addInputPath(job, new Path(Common.getPath(args[i])));
        }
        job.addCacheFile(new Path(Common.getPath(args[0])).toUri());
        Common.delete(args[args.length - 1], conf);
        FileOutputFormat.setOutputPath(job, new Path(Common.getPath(args[args.length - 1])));
        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
}
