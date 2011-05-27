/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/**
 * A enumeration of compare operations.
 */
//## Java 1.6 begin ##
public enum CompareType {
    EQUAL("=", true),
    BIGGER(">", true),
    BIGGER_EQUAL(">=", true),
    SMALLER("<", true),
    SMALLER_EQUAL("<=", true),
    NOT_EQUAL("<>", true),
    IS_NOT_NULL("IS NOT NULL", false),
    IS_NULL("IS NULL", false),
    LIKE("LIKE", true);

    private String text;
    private boolean hasRightExpression;
    
    CompareType(String text, boolean hasRightExpression) {
        this.text = text;
        this.hasRightExpression = hasRightExpression;
    }
    
    public String toString() {
        return text;
    }
    
    public boolean hasRightExpression() {
        return hasRightExpression;
    }
    
}
//## Java 1.6 end ##

