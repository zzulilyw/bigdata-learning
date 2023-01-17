package com.lyw.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import java.io.*;

public class MySerial {
    public final static String filePath = "/Users/lyw/IdeaProjects/bigdata-learning/src/main/resources/tmp/java/serial";
    public final static String filePathHadoop = "/Users/lyw/IdeaProjects/bigdata-learning/src/main/resources/tmp/hadoop/serial";

    /**
     * Java序列化
     * @throws IOException
     */
    @Test
    public void testJavaSerialize() throws IOException {
        Integer num = 2022;
        FileOutputStream fos = new FileOutputStream(filePath);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeInt(num);
        oos.close();
        fos.close();
    }

    /**
     * Java序列化
     * @throws IOException
     */
    @Test
    public void testHadoopSerialize() throws IOException {
        IntWritable iw = new IntWritable(2022);
        FileOutputStream fos = new FileOutputStream(filePathHadoop);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        DataOutputStream dos = new DataOutputStream(oos);
        // java序列化
        iw.write(dos);
        dos.close();
        fos.close();
    }

    @Test
    public void testJavaDeserialize() throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(filePath);
        ObjectInputStream ois = new ObjectInputStream(fis);
        int o = ois.readInt();
        System.out.println(o);
    }

    @Test
    public void testHadoopDeserialize() throws IOException, ClassNotFoundException {
        IntWritable iw = new IntWritable();
        FileInputStream fis = new FileInputStream(filePathHadoop);
        ObjectInputStream ois = new ObjectInputStream(fis);
        iw.readFields(ois);
        int res = iw.get();
        System.out.println(res);
    }

}
