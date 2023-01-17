package com.lyw.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IpCount {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(IpCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);
        job.setJobName("IpCount job");


    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text outKey = new Text();
        private IntWritable outValue = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String s = value.toString().split(" ")[0];
            outKey.set(s);
            context.write(outKey, outValue);
        }
    }

    public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        private Text outKey = new Text();
        private IntWritable outValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            outKey.set(key);
            outValue.set(sum);
            context.write(outKey, outValue);
        }
    }
}
