/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.test.jaqu;

//## Java 1.5 begin ##
import java.util.Arrays;
import java.util.List;
import org.h2.jaqu.Table.JQColumn;
import org.h2.jaqu.Table.JQTable;
//## Java 1.5 end ##

/**
 * A table containing product data.
 */
//## Java 1.5 begin ##
@JQTable(createIfRequired = false)
public class ProductNoCreateTable {

    @SuppressWarnings("unused")
    @JQColumn(name = "id")
    private Integer productId;

    @SuppressWarnings("unused")
    @JQColumn(name = "name")
    private String productName;

    public ProductNoCreateTable() {
        // public constructor
    }

    private ProductNoCreateTable(int productId, String productName) {
        this.productId = productId;
        this.productName = productName;
    }

    private static ProductNoCreateTable create(int productId, String productName) {
        return new ProductNoCreateTable(productId, productName);
    }

    public static List<ProductNoCreateTable> getList() {
        ProductNoCreateTable[] list = { create(1, "Chai"), create(2, "Chang") };
        return Arrays.asList(list);
    }

}
//## Java 1.5 end ##
