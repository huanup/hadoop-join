package com.mine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * Created by zhanghuan on 2018/12/10.
 * 工资最高的前三个人
 */
public class Top3Salary {

    static class TopMap extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] item = value.toString().split(",");
            //输出工资和员工姓名
            context.write(new LongWritable(Long.parseLong(item[5])), new Text(item[1]));
        }
    }


    static class TopReduce extends Reducer<LongWritable, Text, Text, LongWritable> {
        //reduce 进程中进行计数 取出前三
        private int i = 0;
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text text : values) {
                if (i == 3) {
                    break;
                }
                context.write(text, key);
                i ++;
            }
        }
    }

    /**
     * map阶段倒序排序
     */
    static class TopComparator extends LongWritable.Comparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("args length must >= 2");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top 3 salary");
        job.setJarByClass(Top3Salary.class);
        job.setMapperClass(TopMap.class);
        job.setReducerClass(TopReduce.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        //指定reducer个数 实现工资全局倒序排序
        job.setNumReduceTasks(1);
        job.setSortComparatorClass(TopComparator.class);
        for (int i = 0, len = args.length - 1; i < len; i++) {
            FileInputFormat.addInputPath(job, new Path(Common.getPath(args[i])));
        }
        Common.delete(args[args.length - 1], conf);
        FileOutputFormat.setOutputPath(job, new Path(Common.getPath(args[args.length - 1])));
        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
}
