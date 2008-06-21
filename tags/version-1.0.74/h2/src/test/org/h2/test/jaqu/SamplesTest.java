/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jaqu;

//## Java 1.6 begin ##
import java.math.BigDecimal;
import java.util.List;

import org.h2.jaqu.Db;
import org.h2.test.TestBase;
//## Java 1.6 end ##

/**
 * Implementation of the 101 LINQ Samples as described in
 * http://msdn2.microsoft.com/en-us/vcsharp/aa336760.aspx
 */
//## Java 1.6 begin ##
public class SamplesTest extends TestBase {
    
    private Db db;
    
    public void test() throws Exception {
        db = Db.open("jdbc:h2:mem:", "sa", "sa");
        db.insertAll(Product.getProductList());
        db.insertAll(Customer.getCustomerList());
        db.insertAll(Order.getOrderList());
        testSelectManyCompoundFrom2();
        testAnonymousTypes3();
        testWhereSimple4();
        testWhereSimple2();
        testWhereSimple3();
        testSelectSimple2();
        db.close();
    }

    private void testWhereSimple2() throws Exception {
        
//            var soldOutProducts =
//                from p in products
//                where p.UnitsInStock == 0
//                select p;

        Product p = db.alias(Product.class);
        List<Product> soldOutProducts = 
            db.from(p).
            where(p.unitsInStock).is(0).
            orderBy(p.productId).select();
        
        String result = "";
        for (Product x : soldOutProducts) {
            result += x.productName + ";";
        }
        assertEquals(result, "Chef Anton's Gumbo Mix;Alice Mutton;"
            + "Thueringer Rostbratwurst;Gorgonzola Telino;Perth Pasties;");
    }

    private void testWhereSimple3() throws Exception {
        
//            var expensiveInStockProducts =
//                from p in products
//                where p.UnitsInStock > 0 
//                && p.UnitPrice > 3.00M
//                select p;
        
        Product p = db.alias(Product.class);
        List<Product> expensiveInStockProducts = 
            db.from(p).
            where(p.unitsInStock).isBigger(0).
            and(p.unitPrice).isBigger(3.0).
            orderBy(p.productId).select();
        
        String result = "";
        for (Product x : expensiveInStockProducts) {
            result += x.productName + ";";
        }
        assertEquals(
                result,
                "Chai;Chang;Aniseed Syrup;Chef Anton's Cajun Seasoning;"
                        + "Grandma's Boysenberry Spread;"
                        + "Uncle Bob's Organic Dried Pears;"
                        + "Northwoods Cranberry Sauce;Mishi Kobe Niku;Ikura;"
                        + "Queso Cabrales;Queso Manchego La Pastora;"
                        + "Konbu;Tofu;Genen Shouyu;Pavlova;"
                        + "Carnarvon Tigers;Teatime Chocolate Biscuits;"
                        + "Sir Rodney's Marmalade;Sir Rodney's Scones;"
                        + "Gustaf's Knaeckebroed;Tunnbroed;Guarana Fantastica;"
                        + "NuNuCa Nuss-Nougat-Creme;Gumbaer Gummibaerchen;"
                        + "Schoggi Schokolade;Roessle Sauerkraut;"
                        + "Nord-Ost Matjeshering;Mascarpone Fabioli;Sasquatch Ale;"
                        + "Steeleye Stout;Inlagd Sill;Gravad lax;Cote de Blaye;"
                        + "Chartreuse verte;Boston Crab Meat;Jack's New England Clam Chowder;"
                        + "Singaporean Hokkien Fried Mee;Ipoh Coffee;Gula Malacca;Rogede sild;"
                        + "Spegesild;Zaanse koeken;Chocolade;Maxilaku;Valkoinen suklaa;"
                        + "Manjimup Dried Apples;Filo Mix;Tourtiere;Pate chinois;"
                        + "Gnocchi di nonna Alice;Ravioli Angelo;Escargots de Bourgogne;"
                        + "Raclette Courdavault;Camembert Pierrot;Sirop d'erable;"
                        + "Tarte au sucre;Vegie-spread;Wimmers gute Semmelknoedel;"
                        + "Louisiana Fiery Hot Pepper Sauce;Louisiana Hot Spiced Okra;"
                        + "Laughing Lumberjack Lager;Scottish Longbreads;Gudbrandsdalsost;"
                        + "Outback Lager;Flotemysost;Mozzarella di Giovanni;Roed Kaviar;"
                        + "Longlife Tofu;Rhoenbraeu Klosterbier;Lakkalikoeoeri;"
                        + "Original Frankfurter gruene Sosse;");
    }


    private void testWhereSimple4() throws Exception {
        
//        var waCustomers =
//            from c in customers
//            where c.Region == "WA"
//            select c;
        
        Customer c = db.alias(Customer.class);
        List<Customer> waCustomers = 
            db.from(c).
            where(c.region).is("WA").
            select();
        
        for (Customer cu : waCustomers) {
            assertEquals("WA", cu.region);
        }
    }

    private void testSelectSimple2() throws Exception {
        
//        var productNames =
//            from p in products
//            select p.ProductName;
        
        Product p = db.alias(Product.class);
        List<String> productNames = 
            db.from(p).
            orderBy(p.productId).select(p.productName);
        
        List<Product> products = Product.getProductList();
        for (int i = 0; i < products.size(); i++) {
            assertEquals(products.get(i).productName, productNames.get(i));
        }
    }
//## Java 1.6 end ##

    /**
     * A result set class containing the product name and price.
     */
//## Java 1.6 begin ##
    public static class ProductPrice {
        public String productName;
        public String category;
        public Double price;
    }

    private void testAnonymousTypes3() throws Exception {
        
//        var productInfos =
//            from p in products
//            select new {
//                p.ProductName, 
//                p.Category, 
//                Price = p.UnitPrice
//            };
        
        final Product p = db.alias(Product.class);
        List<ProductPrice> productInfos = 
            db.from(p).orderBy(p.productId).
            select(new ProductPrice() { {
                    productName = p.productName;
                    category = p.category;
                    price = p.unitPrice;
            }});
        
        List<Product> products = Product.getProductList();
        assertEquals(products.size(), productInfos.size());
        for (int i = 0; i < products.size(); i++) {
            ProductPrice pr = productInfos.get(i);
            Product p2 = products.get(i);
            assertEquals(p2.productName, pr.productName);
            assertEquals(p2.category, pr.category);
            assertEquals(p2.unitPrice, pr.price);
        }
    }
//## Java 1.6 end ##

    /**
     * A result set class containing customer data and the order total.
     */    
//## Java 1.6 begin ##
    public static class CustOrder {
        public String customerId;
        public Integer orderId;
        public BigDecimal total;
    }
    
    private void testSelectManyCompoundFrom2() {
        
//        var orders =
//            from c in customers,
//            o in c.Orders
//            where o.Total < 500.00M
//            select new {
//                c.CustomerID, 
//                o.OrderID, 
//                o.Total
//            };
        
        final Customer c = db.alias(Customer.class);
        final Order o = db.alias(Order.class);
        List<CustOrder> orders = 
            db.from(c).
            innerJoin(o).on(c.customerId).is(o.customerId).
            where(o.total).isSmaller(new BigDecimal("500.00")).
            select(new CustOrder() { {
                customerId = c.customerId;
                orderId = o.orderId;
                total = o.total;
            }});
        
        StringBuilder buff = new StringBuilder();
        for (CustOrder co : orders) {
            buff.append("c:");
            buff.append(co.customerId);
            buff.append("/o:");
            buff.append(co.orderId);
            buff.append(';');
        }
        String s = buff.toString();
        System.out.println(s);
        // int todoImplementListResolution;
        // int todoVerifyResult;
    }

}
//## Java 1.6 end ##
