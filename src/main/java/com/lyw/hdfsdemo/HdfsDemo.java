package com.lyw.hdfsdemo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class HdfsDemo {

    private static Configuration conf = null;
    private static FileSystem fs = null;

    @Before
    public void connectionToHdfs() throws IOException {
        // 设置执行用户
        System.setProperty("HADOOP_USER_NAME","hadoop");
        conf = new Configuration();
        // 单点配置
//        conf.set("fs.defaultFS","hdfs://ds-bigdata-002:8020");

        // HA模式的配置
        conf.set("fs.defaultFS","hdfs://ds");
        conf.set("dfs.nameservices","ds");
        conf.set("dfs.ha.namenodes.ds","namenode39,namenode41");
        conf.set("dfs.namenode.rpc-address.ds.namenode39","ds-bigdata-001:8020");
        conf.set("dfs.namenode.rpc-address.ds.namenode41","ds-bigdata-002:8020");
        conf.set("dfs.client.failover.proxy.provider.ds","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        fs = FileSystem.get(conf);
    }

//    @Test
//    public void myHdfsDemo() throws IOException{
//        // 1。建立链接
//        // 1。1 conf设置
//        // 创建文件夹
//        fs.mkdirs(new Path("/tmp/liuyawei1208/test0110"));
//    }

    @Test
    public void mkdir() throws IOException {
        fs.mkdirs(new Path("/tmp/liuyawei1208/test0111"));
    }

    @Test
    public void Myput() throws IOException{
        //数据上传的源地址（本地地址）
        Path src = new Path("/Users/lyw/IdeaProjects/bigdata-learning/pom.xml");
        //数据上传的目标地址（HDFS路径）
        Path dst = new Path("/tmp/liuyawei1208/test0111");
        fs.copyFromLocalFile(src,dst);
    }

    @Test
    public void MyGet() throws IOException{
        //数据的下载来源（HDFS）
        Path src = new Path("/tmp/liuyawei1208/1new.txt");
        //数据的目标地址（本地）
        Path dst = new Path("/Users/lyw/IdeaProjects/bigdata-learning/src/main/resources/tmp/1new.txt");
        //执行下载操作
        fs.copyToLocalFile(src,dst);
    }

    @Test
    public void MyDelete() throws IOException{
        fs.delete(new Path("/tmp/liuyawei1208/pom.xml"),true);
    }

    @Test
    public void MyListDir() throws IOException{
        Path dir_path = new Path("/tmp/liuyawei1208");
        RemoteIterator<LocatedFileStatus> list_file = fs.listFiles(dir_path,true);

        while (list_file.hasNext()){
            LocatedFileStatus status = list_file.next();
            //文件名
            System.out.print(status.getPermission()+" ");
            System.out.print(status.getReplication()+" ");
            System.out.print(status.getOwner()+" ");
            System.out.print(status.getGroup()+" ");
            System.out.print(status.getBlockSize()+" ");
            System.out.print(status.getAccessTime()+" ");
            System.out.println(status.getPath()+" ");
        }
    }

    @After
    public void Myclose() throws IOException {
        if (fs != null){
            fs.close();
        }
    }
}
