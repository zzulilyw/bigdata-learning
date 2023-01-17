package com.lyw.mapreduce;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        // Object输入<key,value>对的key值，文本偏移量。FileInputFormat <0,fghdy fgye gfyee>
        // Text <key,value>的value：具体的文本信息
        // Text 输出<key,value>的Key值，对应的单词
        // IntWritable 输出<key,value>对应的文本信息，1 <fghdy,1>,<fgye,1>,<gfyee,1>
        private final static IntWritable one = new IntWritable(1); // 序列化
        private Text word = new Text(); // 进一步封装序列化内容
        // 核心map的业务逻辑
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // key: 输入数据在原始数据的偏移量
            // value: 具体数据，对应的字符串
            // context：暂存map的结果
            StringTokenizer itr = new StringTokenizer(value.toString(),",");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        // 1，2，3，4分别对应输入的Key，输入的Value，输出的Key，输出的Value
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // key：单词信息
            // value: <hello,1,1,2,3,4>这样的值，对每个次数做一些统计
            // context: reduce的缓冲数据
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result); // <hello,14>
        }
    }

    public static void main(String[] args) throws Exception {
        // 这里的conf和HdfsDemo的conf一致，但是没有经过配置，因此会使用127.0.0.1的文件进行读取
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://ds");
        conf.set("dfs.nameservices","ds");
        conf.set("dfs.ha.namenodes.ds","namenode39,namenode41");
        conf.set("dfs.namenode.rpc-address.ds.namenode39","ds-bigdata-001:8020");
        conf.set("dfs.namenode.rpc-address.ds.namenode41","ds-bigdata-002:8020");
        conf.set("dfs.client.failover.proxy.provider.ds","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        System.setProperty("HADOOP_USER_NAME","liuyawei1208");
        // 配置档，获取程序运行的参数，input/output 文件的路径
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) { // input，output文件的路径
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");//实例化job，job名字
        job.setJarByClass(WordCount.class);//加载程序，反射机制
        job.setMapperClass(TokenizerMapper.class);//定义mapper类
        job.setCombinerClass(IntSumReducer.class);//定义combine类
        job.setReducerClass(IntSumReducer.class);//定义reduce类
        job.setOutputKeyClass(Text.class);//输出Key值类型
        job.setOutputValueClass(IntWritable.class);//输出Value值类型
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }//输入文件的信息
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1); // 是否等待任务完成，完成后退出程序
    }
}