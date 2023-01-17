package com.lyw.mapreduce;

import java.io.*;
public class JavaSerial {
    public final static String fileParh = "/Users/lyw/IdeaProjects/bigdata-learning/src/main/resources/tmp/java/serial";
    public static void main(String[] args) throws Exception {
        studentSerialze();
        studentDeserialize();
    }
    //定义序列化方式
    public static void studentSerialze() throws IOException {
        //实例化对象yzj
        Student yzj = new Student();
        yzj.setName("maoshu.ran");
        yzj.setAge(18);
        yzj.setIsmarry(false);
        FileOutputStream fos = new FileOutputStream(fileParh);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        //进行Java的序列化方式
        oos.writeObject(yzj);
        //释放资源
        oos.close();
        fos.close();    //这里其实可以不用写，因为上面一行释放资源会顺带把它封装的对线下也关流了，不过这行即使咱们写了也是不会报错的！
    }
    //定义反序列化方法
    public static void  studentDeserialize() throws Exception {
        FileInputStream fis = new FileInputStream(fileParh);
        ObjectInputStream ois = new ObjectInputStream(fis);
        //调用反序列化流的方法"readObject()"读取对象，要注意的是反序列话的对象需要存在相应的字节码文件。否则会抛异常
        Object res = ois.readObject();
        //释放资源
        ois.close();
        fis.close();
        System.out.println(res);
    }
}