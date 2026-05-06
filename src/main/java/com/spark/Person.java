package com.spark;

import java.io.Serializable;

public class Person implements Serializable {

    private String name;
    private int age;
    private String department;

    public Person() {
    }

    public Person(String name, int age, String department) {
        this.name = name;
        this.age = age;
        this.department = department;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public String getDepartment() {
        return department;
    }
}
