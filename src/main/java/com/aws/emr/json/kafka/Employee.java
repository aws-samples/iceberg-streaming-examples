package com.aws.emr.json.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Employee {
    @JsonProperty
    private int employeeId;

    @JsonProperty
    private String name;

    public Employee() {}

    public Employee(int employeeId, String name) {
        this.setEmployeeId(employeeId);
        this.setName(name);
    }

    public int getEmployeeId() {
        return employeeId;
    }

    public void setEmployeeId(int employeeId) {
        this.employeeId = employeeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

