/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.test.jaqu;

import java.util.List;
import org.h2.constant.ErrorCode;
import org.h2.jaqu.Db;
import org.h2.jdbc.JdbcSQLException;
import org.h2.test.TestBase;

public class AnnotationsTest extends TestBase {

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
    public static void main(String... args) {
        new AnnotationsTest().test();
    }

    public void test() {
//## Java 1.5 begin ##
//      EventLogger.activateConsoleLogger();        
        db = Db.open("jdbc:h2:mem:", "sa", "sa");
        db.insertAll(Product.getList());
        db.insertAll(ProductAnnotationOnly.getList());
        db.insertAll(ProductMixedAnnotation.getList());
        testProductAnnotationOnly();
        testProductMixedAnnotation();
        testTrimStringAnnotation();
        testCreateTableIfRequiredAnnotation();
        testColumnInheritanceAnnotation();
        db.close();
//      EventLogger.deactivateConsoleLogger();       
//## Java 1.5 end ##
    }

    private void testProductAnnotationOnly() {
        ProductAnnotationOnly p = new ProductAnnotationOnly();
        assertEquals(10, db.from(p).selectCount());
        
        // Test JQColumn.name="cat"
        assertEquals(2, db.from(p).where(p.category).is("Beverages").selectCount());

        // Test JQTable.annotationsOnly=true
        // public String unmappedField is ignored by JaQu
        assertEquals(0, db.from(p).where(p.unmappedField).is("unmapped").selectCount());
        
        // Test JQColumn.autoIncrement=true
        // 10 objects, 10 autoIncremented unique values
        assertEquals(10, db.from(p).selectDistinct(p.autoIncrement).size());
        
        // Test JQTable.primaryKey=id
        int errorCode = 0;
        try {
            db.insertAll(ProductAnnotationOnly.getList());
        } catch (Throwable t) {
            if (t.getCause() instanceof JdbcSQLException) {
                JdbcSQLException s = (JdbcSQLException) t.getCause();
                errorCode = s.getErrorCode();
            }
        }
        assertEquals(errorCode, ErrorCode.DUPLICATE_KEY_1);
    }
    
    private void testProductMixedAnnotation() {
        ProductMixedAnnotation p = new ProductMixedAnnotation();
        
        // Test JQColumn.name="cat"
        assertEquals(2, db.from(p).where(p.category).is("Beverages").selectCount());
        
        // Test JQTable.annotationsOnly=false
        // public String mappedField is reflectively mapped by JaQu
        assertEquals(10, db.from(p).where(p.mappedField).is("mapped").selectCount());
        
        // Test JQColumn.primaryKey=true
        int errorCode = 0;
        try {
            db.insertAll(ProductMixedAnnotation.getList());
        } catch (Throwable t) {
            if (t.getCause() instanceof JdbcSQLException) {
                JdbcSQLException s = (JdbcSQLException) t.getCause();
                errorCode = s.getErrorCode();
            }
        }
        assertEquals(errorCode, ErrorCode.DUPLICATE_KEY_1);
    }

    private void testTrimStringAnnotation() {
        ProductAnnotationOnly p = new ProductAnnotationOnly();
        ProductAnnotationOnly prod = db.from(p).selectFirst();
        String oldValue = prod.category;
        String newValue = "01234567890123456789";
        prod.category = newValue; // 2 chars exceeds field max
        db.update(prod);
        
        ProductAnnotationOnly newProd = db.from(p)
            .where(p.productId)
            .is(prod.productId)
            .selectFirst();
        assertEquals(newValue.substring(0, 15), newProd.category);

        newProd.category = oldValue;
        db.update(newProd);
    }
    
    private void testColumnInheritanceAnnotation() {
        ProductInheritedAnnotation table = new ProductInheritedAnnotation();
        Db db = Db.open("jdbc:h2:mem:", "sa", "sa");        
        List<ProductInheritedAnnotation> inserted = ProductInheritedAnnotation.getData();
        db.insertAll(inserted);
        
        List<ProductInheritedAnnotation> retrieved = db.from(table).select();
        
        for (int j = 0; j < retrieved.size(); j++) {
            ProductInheritedAnnotation i = inserted.get(j);
            ProductInheritedAnnotation r = retrieved.get(j);
            assertEquals(i.category, r.category);
            assertEquals(i.mappedField, r.mappedField);
            assertEquals(i.unitsInStock, r.unitsInStock);
            assertEquals(i.unitPrice, r.unitPrice);
            assertEquals(i.name(), r.name());
            assertEquals(i.id(), r.id());
        }
        db.close();
    }

    private void testCreateTableIfRequiredAnnotation() {
        // Tests JQTable.createTableIfRequired=false
        int errorCode = 0;
        try {
            Db noCreateDb = Db.open("jdbc:h2:mem:", "sa", "sa");
            noCreateDb.insertAll(ProductNoCreateTable.getList());
            noCreateDb.close();
        } catch (Throwable e) {
            if (e.getCause() instanceof JdbcSQLException) {
                JdbcSQLException error = (JdbcSQLException) e.getCause();                
                errorCode = error.getErrorCode();
            }
        }
        assertTrue(errorCode == ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1);
    }
    
}
