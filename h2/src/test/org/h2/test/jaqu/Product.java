/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jaqu;

//## Java 1.5 begin ##
import java.util.Arrays;
import java.util.List;

import org.h2.jaqu.Table;

import static org.h2.jaqu.Define.*;
//## Java 1.5 end ##

/**
 * A table containing product data.
 */
//## Java 1.5 begin ##
public class Product implements Table {

    public Integer productId;
    public String productName;
    public String category;
    public Double unitPrice;
    public Integer unitsInStock;
    
    public Product() {
        // public constructor
    }

    private Product(int productId, String productName, 
            String category, double unitPrice, int unitsInStock) {
        this.productId = productId;
        this.productName = productName;
        this.category = category;
        this.unitPrice = unitPrice;
        this.unitsInStock = unitsInStock;
    }

    public void define() {
        tableName("Product");
        primaryKey(productId);
        index(productName, category);
    }

    private static Product create(int productId, String productName, 
            String category, double unitPrice, int unitsInStock) {
        return new Product(productId, productName, category, 
            unitPrice, unitsInStock);
    }

    public static List<Product> getProductList() {
        Product[] list = new Product[] { 
                create(1, "Chai", "Beverages", 18, 39),
                create(2, "Chang", "Beverages", 19.0, 17), 
                create(3, "Aniseed Syrup", "Condiments", 10.0, 13),
                create(4, "Chef Anton's Cajun Seasoning", "Condiments", 22.0, 53),
                create(5, "Chef Anton's Gumbo Mix", "Condiments", 21.3500, 0),
                create(6, "Grandma's Boysenberry Spread", "Condiments", 25.0, 120),
                create(7, "Uncle Bob's Organic Dried Pears", "Produce", 30.0, 15),
                create(8, "Northwoods Cranberry Sauce", "Condiments", 40.0, 6),
                create(9, "Mishi Kobe Niku", "Meat/Poultry", 97.0, 29), 
                create(10, "Ikura", "Seafood", 31.0, 31),
                create(11, "Queso Cabrales", "Dairy Products", 21.0, 22),
                create(12, "Queso Manchego La Pastora", "Dairy Products", 38.0, 86),
                create(13, "Konbu", "Seafood", 6.0, 24), 
                create(14, "Tofu", "Produce", 23.2500, 35),
                create(15, "Genen Shouyu", "Condiments", 15.50, 39), 
                create(16, "Pavlova", "Confections", 17.4500, 29),
                create(17, "Alice Mutton", "Meat/Poultry", 39.0, 0),
                create(18, "Carnarvon Tigers", "Seafood", 62.50, 42),
                create(19, "Teatime Chocolate Biscuits", "Confections", 9.20, 25),
                create(20, "Sir Rodney's Marmalade", "Confections", 81.0, 40),
                create(21, "Sir Rodney's Scones", "Confections", 10.0, 3),
                create(22, "Gustaf's Knaeckebroed", "Grains/Cereals", 21.0, 104),
                create(23, "Tunnbroed", "Grains/Cereals", 9.0, 61),
                create(24, "Guarana Fantastica", "Beverages", 4.50, 20),
                create(25, "NuNuCa Nuss-Nougat-Creme", "Confections", 14.0, 76),
                create(26, "Gumbaer Gummibaerchen", "Confections", 31.2300, 15),
                create(27, "Schoggi Schokolade", "Confections", 43.90, 49),
                create(28, "Roessle Sauerkraut", "Produce", 45.60, 26),
                create(29, "Thueringer Rostbratwurst", "Meat/Poultry", 123.7900, 0),
                create(30, "Nord-Ost Matjeshering", "Seafood", 25.8900, 10),
                create(31, "Gorgonzola Telino", "Dairy Products", 12.50, 0),
                create(32, "Mascarpone Fabioli", "Dairy Products", 32.0, 9),
                create(33, "Geitost", "Dairy Products", 2.50, 112),
                create(34, "Sasquatch Ale", "Beverages", 14.0, 111),
                create(35, "Steeleye Stout", "Beverages", 18.0, 20), 
                create(36, "Inlagd Sill", "Seafood", 19.0, 112),
                create(37, "Gravad lax", "Seafood", 26.0, 11), 
                create(38, "Cote de Blaye", "Beverages", 263.50, 17),
                create(39, "Chartreuse verte", "Beverages", 18.0, 69),
                create(40, "Boston Crab Meat", "Seafood", 18.40, 123),
                create(41, "Jack's New England Clam Chowder", "Seafood", 9.6500, 85),
                create(42, "Singaporean Hokkien Fried Mee", "Grains/Cereals", 14.0, 26),
                create(43, "Ipoh Coffee", "Beverages", 46.0, 17),
                create(44, "Gula Malacca", "Condiments", 19.4500, 27), 
                create(45, "Rogede sild", "Seafood", 9.50, 5),
                create(46, "Spegesild", "Seafood", 12.0, 95), 
                create(47, "Zaanse koeken", "Confections", 9.50, 36),
                create(48, "Chocolade", "Confections", 12.7500, 15), 
                create(49, "Maxilaku", "Confections", 20.0, 10),
                create(50, "Valkoinen suklaa", "Confections", 16.2500, 65),
                create(51, "Manjimup Dried Apples", "Produce", 53.0, 20),
                create(52, "Filo Mix", "Grains/Cereals", 7.0, 38),
                create(53, "Perth Pasties", "Meat/Poultry", 32.80, 0),
                create(54, "Tourtiere", "Meat/Poultry", 7.4500, 21),
                create(55, "Pate chinois", "Meat/Poultry", 24.0, 115),
                create(56, "Gnocchi di nonna Alice", "Grains/Cereals", 38.0, 21),
                create(57, "Ravioli Angelo", "Grains/Cereals", 19.50, 36),
                create(58, "Escargots de Bourgogne", "Seafood", 13.2500, 62),
                create(59, "Raclette Courdavault", "Dairy Products", 55.0, 79),
                create(60, "Camembert Pierrot", "Dairy Products", 34.0, 19),
                create(61, "Sirop d'erable", "Condiments", 28.50, 113),
                create(62, "Tarte au sucre", "Confections", 49.30, 17),
                create(63, "Vegie-spread", "Condiments", 43.90, 24),
                create(64, "Wimmers gute Semmelknoedel", "Grains/Cereals", 33.2500, 22),
                create(65, "Louisiana Fiery Hot Pepper Sauce", "Condiments", 21.0500, 76),
                create(66, "Louisiana Hot Spiced Okra", "Condiments", 17.0, 4),
                create(67, "Laughing Lumberjack Lager", "Beverages", 14.0, 52),
                create(68, "Scottish Longbreads", "Confections", 12.50, 6),
                create(69, "Gudbrandsdalsost", "Dairy Products", 36.0, 26),
                create(70, "Outback Lager", "Beverages", 15.0, 15),
                create(71, "Flotemysost", "Dairy Products", 21.50, 26),
                create(72, "Mozzarella di Giovanni", "Dairy Products", 34.80, 14),
                create(73, "Roed Kaviar", "Seafood", 15.0, 101), 
                create(74, "Longlife Tofu", "Produce", 10.0, 4),
                create(75, "Rhoenbraeu Klosterbier", "Beverages", 7.7500, 125),
                create(76, "Lakkalikoeoeri", "Beverages", 18.0, 57),
                create(77, "Original Frankfurter gruene Sosse", "Condiments", 13.0, 32),
        };

        return Arrays.asList(list);
    }
    
    public String toString() {
        return productName + ": " + unitsInStock;
    }

}
//## Java 1.5 end ##
