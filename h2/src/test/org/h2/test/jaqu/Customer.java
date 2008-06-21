package org.h2.test.jaqu;

//## Java 1.6 begin ##
import java.util.Arrays;
import java.util.List;
//## Java 1.6 end ##

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

//## Java 1.6 begin ##
    public static List<Customer> getCustomerList() {
        Customer[] list = new Customer[] { 
                new Customer("ALFKI", "WA"), 
                new Customer("ANATR", "WA"),
                new Customer("ANTON", "CA") };
        return Arrays.asList(list);
    }
//## Java 1.6 end ##
}
