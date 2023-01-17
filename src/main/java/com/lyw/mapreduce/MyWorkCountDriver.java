package com.lyw.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class MyWorkCountDriver {
    public static void main(String[] args) throws IOException {
        // 读取配置环境信息
        Configuration conf = new Configuration();
        // 1.通过配置文件获取job实例
        Job job = Job.getInstance(conf);
        // 2.指定程序jar的本地路径
        job.setJarByClass(MyWorkCountDriver.class);
        // 3.指定Mapper/reducer类


    }
}
