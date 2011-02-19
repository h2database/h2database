/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu;

import static org.h2.jaqu.util.StringUtils.isNullOrEmpty;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.h2.jaqu.TableDefinition.FieldDefinition;

/**
 * Utility methods for models related to type mapping, default value validation,
 * and class or field name creation. 
 */
public class ModelUtils {

    /**
     * Returns a SQL type mapping for a Java class.
     * 
     * @param field the field to map
     * @param strictTypeMapping throws a RuntimeException if type is unsupported
     * @return
     */
    public static String getDataType(FieldDefinition fieldDef, boolean strictTypeMapping) {        
        Class<?> fieldClass = fieldDef.field.getType();
        if (supportedTypes.containsKey(fieldClass)) {
            String sqltype = supportedTypes.get(fieldClass);
            if (sqltype.equals("VARCHAR") && fieldDef.maxLength <= 0)
                // Unspecified length strings are TEXT, not VARCHAR
                return "TEXT";
            return sqltype;
        }
        if (!strictTypeMapping)
            return "VARCHAR";
        else
            throw new RuntimeException("Unsupported type " + fieldClass.getName());
    }
    
    @SuppressWarnings("serial")
    // Used by Runtime Mapping for CREATE statements
    static Map<Class<?>, String> 
        supportedTypes = new HashMap<Class<?>, String>() {
        {
            put(String.class, "VARCHAR");
            put(Boolean.class, "BIT");
            put(Byte.class, "TINYINT");
            put(Short.class, "SMALLINT");
            put(Integer.class, "INT");
            put(Long.class, "BIGINT");
            put(Float.class, "REAL");
            put(Double.class, "DOUBLE");
            put(BigDecimal.class, "DECIMAL");
            put(java.sql.Timestamp.class, "TIMESTAMP");
            put(java.util.Date.class, "TIMESTAMP");
            put(java.sql.Date.class, "DATE");
            put(java.sql.Time.class, "TIME");
            // TODO add blobs, binary types, custom types?
        }
    };

   
    /**
     * Returns the Java class type for a given SQL type.
     * 
     * @param sqlType
     * @param dateClazz the preferred date class (java.util.Date or java.sql.Timestamp) 
     * @return
     */
    public static Class<?> getClassType(String sqlType, 
            Class<? extends java.util.Date> dateClazz) {
        sqlType = sqlType.toUpperCase();
        // FIXME dropping "UNSIGNED" or parts like that.  could be trouble.
        sqlType = sqlType.split(" ")[0].trim();
        
        if (sqlTypes.containsKey(sqlType))
            // Marshall sqlType to a standard type
            sqlType = sqlTypes.get(sqlType);
        Class<?> mappedClazz = null;
        for (Class<?> clazz : supportedTypes.keySet())
            if (supportedTypes.get(clazz).equalsIgnoreCase(sqlType)) {                
                mappedClazz = clazz;
                break;
            }
        if (mappedClazz != null) {
            if (mappedClazz.equals(java.util.Date.class)
                    || mappedClazz.equals(java.sql.Timestamp.class))
                return dateClazz;                
            return mappedClazz;
        }
        return null;
    }
   

    // Marshall SQL type aliases to the list of supported types.
    // Used by Generation and Validation
    static Map<String, String> sqlTypes = new HashMap<String, String>() {
        {
            // Strings
            put("CHAR", "VARCHAR");
            put("CHARACTER", "VARCHAR");
            put("NCHAR", "VARCHAR");
            put("VARCHAR_CASESENSITIVE", "VARCHAR");
            put("VARCHAR_IGNORECASE", "VARCHAR");
            put("LONGVARCHAR", "VARCHAR");
            put("VARCHAR2", "VARCHAR");
            put("NVARCHAR", "VARCHAR");
            put("NVARCHAR2", "VARCHAR");
            put("TEXT", "VARCHAR");
            put("NTEXT", "VARCHAR");
            put("TINYTEXT", "VARCHAR");
            put("MEDIUMTEXT", "VARCHAR");
            put("LONGTEXT", "VARCHAR");
            put("CLOB", "VARCHAR");
            put("NCLOB", "VARCHAR");
            
            // Logic
            put("BOOL", "BIT");
            put("BOOLEAN", "BIT");
            
            // Whole Numbers
            put("BYTE", "TINYINT");
            put("INT2", "SMALLINT");
            put("YEAR", "SMALLINT");
            put("INTEGER", "INT");
            put("MEDIUMINT", "INT");
            put("INT4", "INT");
            put("SIGNED", "INT");
            put("INT8", "BIGINT");
            put("IDENTITY", "BIGINT");
            
            // Decimals
            put("NUMBER", "DECIMAL");
            put("DEC", "DECIMAL");
            put("NUMERIC", "DECIMAL");
            put("FLOAT", "DOUBLE");
            put("FLOAT4", "DOUBLE");
            put("FLOAT8", "DOUBLE");
            
            // Dates
            put("DATETIME", "TIMESTAMP");
            put("SMALLDATETIME", "TIMESTAMP");
        }
    };
    
    
    /**
     * Tries to create a CamelCase class name from a table.
     * 
     * @param name
     * @return
     */
    public static String createClassName(String name) {
        String[] chunks = name.split("_");
        StringBuilder newName = new StringBuilder();
        for (String chunk : chunks) {
            if (chunk.length() == 0)
                // leading or trailing _
                continue;
            newName.append(Character.toUpperCase(chunk.charAt(0)));
            newName.append(chunk.substring(1).toLowerCase());
        }
        return newName.toString();
    }
    
    /**
     * Ensures that table column names don't collide with Java keywords.
     * 
     * @param col
     * @return
     */
    public static String createFieldName(String col) {
        String cn = col.toLowerCase();
        if (keywords.contains(cn))
            cn += "_value";
        return cn;
    }
    
    @SuppressWarnings("serial")
    static List<String> keywords = new ArrayList<String>() {
        {
            add("abstract");
            add("assert");
            add("boolean");
            add("break");
            add("byte");
            add("case");
            add("catch");
            add("char");
            add("class");
            add("const");
            add("continue");
            add("default");
            add("do");
            add("double");
            add("else");
            add("enum");
            add("extends");
            add("final");
            add("finally");
            add("float");
            add("for");
            add("goto");
            add("if");
            add("implements");
            add("import");
            add("instanceof");
            add("int");
            add("interface");
            add("long");
            add("native");
            add("new");
            add("package");
            add("private");
            add("protected");
            add("public");
            add("return");
            add("short");
            add("static");
            add("strictfp");
            add("super");
            add("switch");
            add("synchronized");
            add("this");
            add("throw");
            add("throws");
            add("transient");
            add("try");
            add("void");
            add("volatile");
            add("while");
            add("false");
            add("null");
            add("true");
        }
    };
    
    /**
     * Checks the formatting of JQColumn.defaultValue()
     * @param defaultValue
     * @return
     */
    public static boolean isProperlyFormattedDefaultValue(String defaultValue) {
        if (isNullOrEmpty(defaultValue))
            return true;
        Pattern literalDefault = Pattern.compile("'.*'");
        Pattern functionDefault = Pattern.compile("[^'].*[^']");
        return literalDefault.matcher(defaultValue).matches() 
            || functionDefault.matcher(defaultValue).matches(); 
    }
    
    /**
     * Checks to see if the defaultValue matches the Class.
     * 
     * @param modelClazz
     * @param defaultValue
     * @return
     */
    public static boolean isValidDefaultValue(Class<?> modelClazz, 
            String defaultValue) {
        
        if (defaultValue == null)
            // NULL
            return true;
        if (defaultValue.trim().length() == 0)
            // NULL (effectively)
            return true;
        
        // FIXME H2 single-quotes literal values.  Very Useful.
        // MySQL does not single-quote literal values so its hard to
        // differentiate a FUNCTION/VARIABLE from a literal value.  
        
        // Function/Variable
        Pattern functionDefault = Pattern.compile("[^'].*[^']");
        if (functionDefault.matcher(defaultValue).matches())
            // Hard to validate this since its in the DB.  Assume its good.
            return true;
        
        // STRING
        if (modelClazz == String.class) {           
            Pattern stringDefault = Pattern.compile("'(.|\\n)*'");
            return stringDefault.matcher(defaultValue).matches();
        }
        
        String dateRegex = "[0-9]{1,4}[-/\\.][0-9]{1,2}[-/\\.][0-9]{1,2}";
        String timeRegex = "[0-2]{1}[0-9]{1}:[0-5]{1}[0-9]{1}:[0-5]{1}[0-9]{1}";
        
        // TIMESTAMPs
        if (modelClazz == java.util.Date.class 
                || modelClazz == java.sql.Timestamp.class){
            // This may be a little loose....
            // 00-00-00 00:00:00
            // 00/00/00T00:00:00
            // 00.00.00T00:00:00
            Pattern pattern = Pattern.compile("'" + dateRegex + "." + timeRegex + "'");
            return pattern.matcher(defaultValue).matches();
        }
        
        // DATE
        if (modelClazz == java.sql.Date.class) {
            // This may be a little loose....
            // 00-00-00
            // 00/00/00
            // 00.00.00
            Pattern pattern = Pattern.compile("'" + dateRegex + "'");
            return pattern.matcher(defaultValue).matches();
        }

        // TIME
        if (modelClazz == java.sql.Time.class) {
            // 00:00:00
            Pattern pattern = Pattern.compile("'" + timeRegex + "'");
            return pattern.matcher(defaultValue).matches();        
        }
        
        // NUMBER
        if (Number.class.isAssignableFrom(modelClazz)) {
            // Strip single quotes
            String unquoted = defaultValue;
            if (unquoted.charAt(0) == '\'')
                unquoted = unquoted.substring(1);
            if (unquoted.charAt(unquoted.length() - 1) == '\'')
                unquoted = unquoted.substring(0, unquoted.length() - 1);
            
            try {
                // Delegate to static valueOf() method to parse string
                Method m = modelClazz.getMethod("valueOf", String.class);
                Object o = m.invoke(null, unquoted);
            } catch (NumberFormatException nex) {
                return false;
            } catch (Throwable t) {               
            }            
        }
        return true;
    }
}
