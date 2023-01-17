package com.lyw.mapreduce;

import java.io.*;

public class HadoopSerial {
    public final static String fileParh = "/Users/lyw/IdeaProjects/bigdata-learning/src/main/resources/tmp/hadoop/serial";
    public static void main(String[] args) throws IOException {
        studentSerialze();
        studentDeserialize();
    }
    //定义序列化方式
    public static void studentSerialze() throws IOException {
        //实例化对象yzj
        Student rms = new Student();
        rms.setName("maoshu.ran");
        rms.setAge(18);
        rms.setIsmarry(false);
        //初始化StudentWirtable，这是咱们定义的一个容器
        StudentWirtable sw = new StudentWirtable();
        sw.setStudent(rms);
        FileOutputStream fos = new FileOutputStream(fileParh);
        DataOutputStream dos = new DataOutputStream(fos);
        //进行Hadoop的序列化方式
        sw.write(dos);
        //别忘记释放资源哟
        dos.close();
        fos.close();
    }
    //定义反序列化方式
    public static void  studentDeserialize() throws IOException {
        //初始化intWritable
        StudentWirtable sw = new StudentWirtable();
        DataInputStream dis = new DataInputStream(new FileInputStream(fileParh));
        sw.readFields(dis);
        Student rms = sw.getStudent();
        dis.close();
        System.out.println(rms.toString());

    }
}