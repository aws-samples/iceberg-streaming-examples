package com.aws.emr.json.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Employee {
    @JsonProperty
    public String firstName;

    @JsonProperty
    public String lastName;

    @JsonProperty
    public short age;

    public Employee() {}

    public Employee(String firstName, String lastName, short age) {
      //  this(firstName, lastName, age, null);
    }
}
