package com.mine;

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

/**
 * Created by zhanghuan on 2018/12/12
 * 员工工资排序 从小到大 实现全局排序
 */

public class EmpSalarySort {

    static class EmpMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] item = value.toString().split(",");
            //map阶段天然排序 通过限制reduce个数实现全局排序
            context.write(new LongWritable(Long.parseLong(item[5])), new Text(item[1]));
        }
    }

    static class EmpReduce extends Reducer<LongWritable, Text, Text, LongWritable>{
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text text : values){
                System.err.println(key.toString());
                context.write(text, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("args length must >= 2");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "emp salary sort");
        job.setJarByClass(EmpSalarySort.class);
        job.setMapperClass(EmpMapper.class);
        job.setReducerClass(EmpReduce.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        //指定reduce个数
        job.setNumReduceTasks(1);
        for (int i = 0, len = args.length - 1; i < len; i++) {
            FileInputFormat.addInputPath(job, new Path(Common.getPath(args[i])));
        }
        Common.delete(args[args.length - 1], conf);
        FileOutputFormat.setOutputPath(job, new Path(Common.getPath(args[args.length - 1])));
        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
}
