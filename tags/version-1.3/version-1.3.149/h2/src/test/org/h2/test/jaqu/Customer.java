/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jaqu;

//## Java 1.5 begin ##
import java.util.Arrays;
import java.util.List;
//## Java 1.5 end ##

/**
 * A table containing customer data.
 */
public class Customer {

    public String customerId;
    public String region;

    public Customer() {
        // public constructor
    }

    public Customer(String customerId, String region) {
        this.customerId = customerId;
        this.region = region;
    }

    public String toString() {
        return customerId;
    }

//## Java 1.5 begin ##
    public static List<Customer> getList() {
        Customer[] list = {
                new Customer("ALFKI", "WA"),
                new Customer("ANATR", "WA"),
                new Customer("ANTON", "CA") };
        return Arrays.asList(list);
    }
//## Java 1.5 end ##
}
