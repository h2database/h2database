/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java;

import java.util.ArrayList;

/**
 * An expression.
 */
public interface Expr {
    // toString
}

/**
 * A method call.
 */
class CallExpr implements Expr {
    String object;
    ArrayList<Expr> args = new ArrayList<Expr>();
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append(JavaParser.toC(object)).append("(");
        int i = 0;
        for (Expr a : args) {
            if (i > 0) {
                buff.append(", ");
            }
            i++;
            buff.append(a);
        }
        return buff.append(")").toString();
    }
}

/**
 * A assignment expression.
 */
class AssignExpr implements Expr {
    Expr left;
    String op;
    Expr right;

    public String toString() {
        return left + " " + op + " " + right;
    }
}

/**
 * A conditional expression.
 */
class ConditionalExpr implements Expr {
    Expr condition;
    Expr ifTrue, ifFalse;
    public String toString() {
        return condition + " ? " + ifTrue + " : " + ifFalse;
    }
}

/**
 * A literal.
 */
class LiteralExpr implements Expr {
    String literal;
    public String toString() {
        return literal;
    }
}

/**
 * An operation.
 */
class OpExpr implements Expr {
    Expr left;
    String op;
    Expr right;
    public String toString() {
        if (left == null) {
            return op + right;
        } else if (right == null) {
            return left + op;
        }
        return left + " " + op + " " + right;
    }
}

/**
 * A "new" expression.
 */
class NewExpr implements Expr {
    String className;
    public String toString() {
        return "new " + className;
    }
}

/**
 * A String literal.
 */
class StringExpr implements Expr {

    /**
     * The literal.
     */
    String text;

    public String toString() {
        return "\"" + javaEncode(text) + "\"";
    }

    /**
     * Encode the String to Java syntax.
     *
     * @param s the string
     * @return the encoded string
     */
    static String javaEncode(String s) {
        StringBuilder buff = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
            case '\t':
                // HT horizontal tab
                buff.append("\\t");
                break;
            case '\n':
                // LF linefeed
                buff.append("\\n");
                break;
            case '\f':
                // FF form feed
                buff.append("\\f");
                break;
            case '\r':
                // CR carriage return
                buff.append("\\r");
                break;
            case '"':
                // double quote
                buff.append("\\\"");
                break;
            case '\\':
                // backslash
                buff.append("\\\\");
                break;
            default:
                int ch = c & 0xffff;
                if (ch >= ' ' && (ch < 0x80)) {
                    buff.append(c);
                // not supported in properties files
                // } else if(ch < 0xff) {
                // buff.append("\\");
                // // make sure it's three characters (0x200 is octal 1000)
                // buff.append(Integer.toOctalString(0x200 | ch).substring(1));
                } else {
                    buff.append("\\u");
                    // make sure it's four characters
                    buff.append(Integer.toHexString(0x10000 | ch).substring(1));
                }
            }
        }
        return buff.toString();
    }
}
