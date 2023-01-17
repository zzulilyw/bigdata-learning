package com.lyw.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyWorkCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text text = new Text();
    IntWritable intWritable = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split(" ");
        for (String s1 : s) {
            text.set(s1);
            // 正常是使用map.get(key)++，为什么这个地方放置的value永远都是1？后面做了哪些业务上的操作？
            context.write(text, intWritable);
        }
    }
}
