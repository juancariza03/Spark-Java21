package com.spark;

import java.io.Serializable;

public record Person(String name, int age, String department) implements Serializable {
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
