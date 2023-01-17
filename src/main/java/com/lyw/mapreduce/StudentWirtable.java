package com.lyw.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class StudentWirtable implements Writable {
    //切记这里需要给student赋值，不然可能会报错空指针异常哟！
    private Student student = new Student();
    public Student getStudent() {
        return student;
    }
    public void setStudent(Student student) {
        this.student = student;
    }
    //定义串行化的方法
    public void write(DataOutput dataOutput) throws IOException {
        //定义自定义类的序列化顺序，我这里是先序列化name，在序列化age，最好才序列化ismarry。
        dataOutput.writeUTF(student.getName());
        dataOutput.writeInt(student.getAge());
        dataOutput.writeBoolean(student.isIsmarry());
    }
    //定义反串行化的方法
    public void readFields(DataInput dataInput) throws IOException {
        student.setName(dataInput.readUTF());
        student.setAge(dataInput.readInt());
        student.setIsmarry(dataInput.readBoolean());
    }
}