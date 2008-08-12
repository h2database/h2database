/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jaqu;

import java.math.BigDecimal;
//## Java 1.5 begin ##
import java.util.List;

import org.h2.jaqu.Db;
import static org.h2.jaqu.Function.*;
//## Java 1.5 end ##
import org.h2.test.TestBase;

/**
 * <p>
 * This is the implementation of the 101 LINQ Samples as described in
 * http://msdn2.microsoft.com/en-us/vcsharp/aa336760.aspx
 * </p><p>Why should you use JaQu? 
 * Type checking, 
 * autocomplete, 
 * no separate SQL scripts, 
 * no more SQL injection.</p>
 */
public class SamplesTest extends TestBase {
    /**
     * This object represents a database (actually a connection to the database).
     */
//## Java 1.5 begin ##
    Db db;
//## Java 1.5 end ##    
    
    /**
     * This method is called when executing this application from the command
     * line.
     * 
     * @param args the command line parameters
     */    
    public static void main(String[] args) throws Exception {
        new SamplesTest().test();
    }
    
    public void test() throws Exception {
//## Java 1.5 begin ##
        db = Db.open("jdbc:h2:mem:", "sa", "sa");
        db.insertAll(Product.getProductList());
        db.insertAll(Customer.getCustomerList());
        db.insertAll(Order.getOrderList());
        // TODO use prepared statements
        // TODO test all relevant data types (Date,...)
        // TODO nested AND/OR, >, <,...
        // TODO NOT
        // TODO +, -, *, /, ||, nested operations
        // TODO LIKE ESCAPE...
        // TODO UPDATE: FROM ... UPDATE?
        // TODO SELECT UNION
        // TODO DatabaseAdapter
        testOrAndNot();
        testDelete();
        testIsNull();
        testLike();
        testMinMax();
        testSum();
        testLength();
        testCount();
        testGroup();
        testSelectManyCompoundFrom2();
        testWhereSimple4();
        testSelectSimple2();
        testAnonymousTypes3();
        testWhereSimple2();
        testWhereSimple3();
        db.close();
//## Java 1.5 end ##
    }

//## Java 1.5 begin ##    
    private void testWhereSimple2() throws Exception {
        
//            var soldOutProducts =
//                from p in products
//                where p.UnitsInStock == 0
//                select p;

        Product p = new Product();
        List<Product> soldOutProducts = 
            db.from(p).
            where(p.unitsInStock).is(0).
            orderBy(p.productId).select();
        
        assertEquals("[Chef Anton's Gumbo Mix: 0]", soldOutProducts.toString());
    }

    private void testWhereSimple3() throws Exception {
        
//            var expensiveInStockProducts =
//                from p in products
//                where p.UnitsInStock > 0 
//                && p.UnitPrice > 3.00M
//                select p;
        
        Product p = new Product();
        List<Product> expensiveInStockProducts = 
            db.from(p).
            where(p.unitsInStock).bigger(0).
            and(p.unitPrice).bigger(30.0).
            orderBy(p.productId).select();
        
        assertEquals("[Northwoods Cranberry Sauce: 6, Mishi Kobe Niku: 29, Ikura: 31]", 
                expensiveInStockProducts.toString());
    }


    private void testWhereSimple4() throws Exception {
        
//        var waCustomers =
//            from c in customers
//            where c.Region == "WA"
//            select c;
        
        Customer c = new Customer();
        List<Customer> waCustomers = 
            db.from(c).
            where(c.region).is("WA").
            select();
        
        assertEquals("[ALFKI, ANATR]", waCustomers.toString());
    }

    private void testSelectSimple2() throws Exception {
        
//        var productNames =
//            from p in products
//            select p.ProductName;
        
        Product p = new Product();
        List<String> productNames = 
            db.from(p).
            orderBy(p.productId).select(p.productName);
        
        List<Product> products = Product.getProductList();
        for (int i = 0; i < products.size(); i++) {
            assertEquals(products.get(i).productName, productNames.get(i));
        }
    }
//## Java 1.5 end ##

    /**
     * A result set class containing the product name and price.
     */
    public static class ProductPrice {
        public String productName;
        public String category;
        public Double price;
    }

//## Java 1.5 begin ##
    private void testAnonymousTypes3() throws Exception {
        
//        var productInfos =
//            from p in products
//            select new {
//                p.ProductName, 
//                p.Category, 
//                Price = p.UnitPrice
//            };
        
        final Product p = new Product();
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
//## Java 1.5 end ##

    /**
     * A result set class containing customer data and the order total.
     */    
    public static class CustOrder {
        public String customerId;
        public Integer orderId;
        public BigDecimal total;
        public String toString() {
            return customerId + ":" + orderId + ":" + total;
        }
    }
    
//## Java 1.5 begin ##
    private void testSelectManyCompoundFrom2() throws Exception {
        
//        var orders =
//            from c in customers,
//            o in c.Orders
//            where o.Total < 500.00M
//            select new {
//                c.CustomerID, 
//                o.OrderID, 
//                o.Total
//            };
        
        final Customer c = new Customer();
        final Order o = new Order();
        List<CustOrder> orders = 
            db.from(c).
            innerJoin(o).on(c.customerId).is(o.customerId).
            where(o.total).smaller(new BigDecimal("100.00")).
            orderBy(1).
            select(new CustOrder() { {
                customerId = c.customerId;
                orderId = o.orderId;
                total = o.total;
            }});
        
        assertEquals("[ANATR:10308:88.80]", orders.toString());
    }
    
    private void testIsNull() throws Exception {
        Product p = new Product();
        String sql = db.from(p).whereTrue(isNull(p.productName)).getSQL();
        assertEquals("SELECT * FROM Product WHERE (productName IS NULL)", sql);
    }
    
    private void testDelete() throws Exception {
        Product p = new Product();
        int deleted = db.from(p).where(p.productName).like("A%").delete();
        assertEquals(1, deleted);
        deleted = db.from(p).delete();
        assertEquals(9, deleted);
        db.insertAll(Product.getProductList());
    }
    
    private void testOrAndNot() throws Exception {
        Product p = new Product();
        String sql = db.from(p).whereTrue(not(isNull(p.productName))).getSQL();
        assertEquals("SELECT * FROM Product WHERE (NOT productName IS NULL)", sql);
        sql = db.from(p).whereTrue(not(isNull(p.productName))).getSQL();
        assertEquals("SELECT * FROM Product WHERE (NOT productName IS NULL)", sql);
        sql = db.from(p).whereTrue(db.test(p.productId).is(1)).getSQL();
        assertEquals("SELECT * FROM Product WHERE ((productId = 1))", sql);
    }
    
    private void testLength() throws Exception {
        Product p = new Product();
        List<Integer> lengths = db.from(p).
            where(length(p.productName)).smaller(10).
            orderBy(1).
            selectDistinct(length(p.productName));
        assertEquals("[4, 5]", lengths.toString());
    }
    
    private void testSum() throws Exception {
        Product p = new Product();
        Integer sum = db.from(p).selectFirst(sum(p.unitsInStock));
        assertEquals(323, sum.intValue());
        Double sumPrice = db.from(p).selectFirst(sum(p.unitPrice));
        assertEquals(313.35, sumPrice.doubleValue());
    }

    private void testMinMax() throws Exception {
        Product p = new Product();
        Integer min = db.from(p).selectFirst(min(p.unitsInStock));
        assertEquals(0, min.intValue());
        String minName = db.from(p).selectFirst(min(p.productName));
        assertEquals("Aniseed Syrup", minName);
        Double max = db.from(p).selectFirst(max(p.unitPrice));
        assertEquals(97.0, max.doubleValue());
    }
    
    private void testLike() throws Exception {
        Product p = new Product();
        List<Product> aList = db.from(p).
            where(p.productName).like("Cha%").
            orderBy(p.productName).select();
        assertEquals("[Chai: 39, Chang: 17]", aList.toString());
    }

    private void testCount() throws Exception {
        long count = db.from(new Product()).selectCount();
        assertEquals(10, count);
    }
    
//## Java 1.5 end ##

    /**
     * A result set class containing product groups.
     */    
    public static class ProductGroup {
        public String category;
        public Long productCount;
        public String toString() {
            return category + ":" + productCount;
        }
    }
    
//## Java 1.5 begin ##
    private void testGroup() throws Exception {
        
//      var orderGroups =
//          from p in products
//          group p by p.Category into g
//          select new { 
//                Category = g.Key, 
//                Products = g 
//          };
        
        final Product p = new Product();
        List<ProductGroup> list = 
            db.from(p).
            groupBy(p.category).
            orderBy(1).
            select(new ProductGroup() { {
                category = p.category;
                productCount = count();
            }});
        
        assertEquals("[Beverages:2, Condiments:5, " + 
                "Meat/Poultry:1, Produce:1, Seafood:1]", 
                list.toString());
    }

//## Java 1.5 end ##
}
