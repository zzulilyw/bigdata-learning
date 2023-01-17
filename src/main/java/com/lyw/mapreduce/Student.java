package com.lyw.mapreduce;

import java.io.Serializable;

public class Student implements Serializable{
    private String Name;
    private  int age;
    private  boolean ismarry;
    public String getName() {
        return Name;
    }
    public int getAge() {
        return age;
    }
    public boolean isIsmarry() {
        return ismarry;
    }
    public void setName(String name) {
        this.Name=name;
    }
    public void setAge(int age) {
        this.age = age;
    }
    public void setIsmarry(boolean ismarry) {
        this.ismarry = ismarry;
    }
    @Override
    public String toString() {
        return "Student{" +
                "name='" + Name + '\'' +
                ", age=" + age +
                ", ismarry=" + ismarry +
                '}';
    }
}