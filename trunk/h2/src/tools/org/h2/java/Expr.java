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
    Type getType();
}

/**
 * A method call.
 */
class CallExpr implements Expr {

    final JavaParser context;
    final Expr expr;
    final ArrayList<Expr> args = new ArrayList<Expr>();
    final boolean isStatic;
    final String className;
    final String name;

    CallExpr(JavaParser context, Expr expr, String className, String name, boolean isStatic) {
        this.context = context;
        this.expr = expr;
        this.className = className;
        this.name = name;
        this.isStatic = isStatic;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        if (className != null) {
            buff.append(JavaParser.toC(className + "." + name)).append("(");
        } else {
            buff.append(JavaParser.toC(expr.getType().type + "." + name)).append("(");
        }
        int i = 0;
        if (expr != null) {
            buff.append(expr.toString());
            i++;
        }
        for (Expr a : args) {
            if (i > 0) {
                buff.append(", ");
            }
            i++;
            buff.append(a);
        }
        return buff.append(")").toString();
    }

    public Type getType() {
        // TODO
        return null;
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

    public Type getType() {
        // TODO
        return null;
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

    public Type getType() {
        // TODO
        return null;
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

    public Type getType() {
        // TODO
        return null;
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

    public Type getType() {
        // TODO
        return null;
    }

}

/**
 * A "new" expression.
 */
class NewExpr implements Expr {

    ClassObj type;
    ArrayList<Expr> arrayInitExpr = new ArrayList<Expr>();

    public String toString() {
        StringBuilder buff = new StringBuilder();
        if (arrayInitExpr.size() > 0) {
            if (type.isPrimitive) {
                buff.append("NEW_ARRAY(sizeof(" + type + ")");
                buff.append(", 1 ");
                for (Expr e : arrayInitExpr) {
                    buff.append("* ").append(e);
                }
                buff.append(")");
            } else {
                buff.append("NEW_OBJ_ARRAY(1 ");
                for (Expr e : arrayInitExpr) {
                    buff.append("* ").append(e);
                }
                buff.append(")");
            }
        } else {
            buff.append("NEW_OBJ(" + type.id + ", " + type + ")");
        }
        return buff.toString();
    }

    public Type getType() {
        Type t = new Type();
        t.type = type;
        t.arrayLevel = arrayInitExpr.size();
        return t;
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
        return "STRING(\"" + javaEncode(text) + "\")";
    }

    public Type getType() {
        // TODO
        return null;
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

/**
 * A variable.
 */
class VariableExpr implements Expr {

    Expr base;
    FieldObj field;
    String name;

    public String toString() {
        StringBuilder buff = new StringBuilder();
        if (field != null && "length".equals(field.name) && base != null && base.getType() != null && base.getType().arrayLevel > 0) {
            buff.append("LENGTH(");
            buff.append(base.toString());
            buff.append(")");
        } else {
            if (base != null) {
                buff.append(base.toString()).append("->");
            }
            if (field != null) {
                if (field.isStatic) {
                    // buff.append(JavaParser.toC(field.type.type.name + "." + field.name));
                    buff.append(JavaParser.toC(name));
                } else {
                    buff.append(field.name);
                }
            } else {
                buff.append(JavaParser.toC(name));
            }
        }
        return buff.toString();
    }

    public Type getType() {
        if (field == null) {
            return null;
        }
        return field.type;
    }

}

/**
 * A array access expression.
 */
class ArrayExpr implements Expr {

    Expr expr;
    ArrayList<Expr> indexes = new ArrayList<Expr>();

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append(expr.toString());
        for (Expr e : indexes) {
            buff.append('[').append(e.toString()).append(']');
        }
        return buff.toString();
    }

    public Type getType() {
        return expr.getType();
    }

}
