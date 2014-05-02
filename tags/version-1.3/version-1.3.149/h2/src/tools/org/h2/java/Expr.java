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
    private ClassObj classObj;
    private MethodObj method;

    CallExpr(JavaParser context, Expr expr, String className, String name, boolean isStatic) {
        this.context = context;
        this.expr = expr;
        this.className = className;
        this.name = name;
        this.isStatic = isStatic;
    }

    private void initMethod() {
        if (method != null) {
            return;
        }
        if (className != null) {
            classObj = context.getClassObj(className);
        } else {
            classObj = expr.getType().classObj;
        }
        method = classObj.getMethod(name, args);
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        String methodName;
        initMethod();
        if (method.isVirtual) {
            methodName = "virtual_" + method.name + "[CLASS_ID("+expr.toString()+")]";
        } else {
            methodName = JavaParser.toC(classObj.toString() + "." + method.name);
        }
        buff.append(methodName).append("(");
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
        initMethod();
        return method.returnType;
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
    private final JavaParser context;
    private final String className;
    private Type type;

    public LiteralExpr(JavaParser context, String className) {
        this.context = context;
        this.className = className;
    }

    public String toString() {
        return literal;
    }

    public Type getType() {
        if (type == null) {
            type = new Type();
            type.classObj = context.getClassObj(className);
        }
        return type;
    }

}

/**
 * An operation.
 */
class OpExpr implements Expr {

    Expr left;
    String op;
    Expr right;
    private final JavaParser context;

    OpExpr(JavaParser context) {
        this.context = context;
    }

    public String toString() {
        if (left == null) {
            return op + right;
        } else if (right == null) {
            return left + op;
        }
        if (op.equals(">>>")) {
            // ujint / ujlong
            return "(((u" + left.getType() + ") " + left + ") >> " + right + ")";
        } else if (op.equals("+")) {
            if (left.getType().isObject() || right.getType().isObject()) {
                // TODO convert primitive to to String, call toString
                StringBuilder buff = new StringBuilder();
                buff.append("java_lang_StringBuilder_toString(");
                buff.append("java_lang_StringBuilder_append(");
                buff.append("java_lang_StringBuilder_init_obj(");
                buff.append(convertToString(left));
                buff.append("), ");
                buff.append(convertToString(right));
                buff.append("))");
                return buff.toString();
            }
        }
        return "(" + left + " " + op + " " + right + ")";
    }

    private String convertToString(Expr e) {
        Type t = e.getType();
        if (t.arrayLevel > 0) {
            return e.toString() + ".toString()";
        }
        if (t.classObj.isPrimitive) {
            ClassObj wrapper = context.getWrapper(t.classObj);
            return JavaParser.toC(wrapper + ".toString") + "(" + e.toString() + ")";
        } else if (e.getType().toString().equals("java_lang_String*")) {
            return e.toString();
        }
        return e.toString() + ".toString()";
    }

    public Type getType() {
        if (left == null) {
            return right.getType();
        }
        if (right == null) {
            return left.getType();
        }
        if (op.equals("+")) {
            if (left.getType().isObject() || right.getType().isObject()) {
                Type t = new Type();
                t.classObj = context.getClassObj("java.lang.String");
                return t;
            }
        }
        Type lt = left.getType();
        Type rt = right.getType();
        if (lt.classObj.primitiveType < rt.classObj.primitiveType) {
            return rt;
        }
        return lt;
    }

}

/**
 * A "new" expression.
 */
class NewExpr implements Expr {

    ClassObj classObj;
    ArrayList<Expr> arrayInitExpr = new ArrayList<Expr>();
    ArrayList<Expr> args = new ArrayList<Expr>();
    final JavaParser context;

    NewExpr(JavaParser context) {
        this.context = context;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        if (arrayInitExpr.size() > 0) {
            if (classObj.isPrimitive) {
                buff.append("NEW_ARRAY(sizeof(" + classObj + ")");
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
            MethodObj m = classObj.getMethod("init_obj", args);
            buff.append(JavaParser.toC(classObj.toString() + "." + m.name)).append("(");
            int i = 0;
            for (Expr a : args) {
                if (i++ > 0) {
                    buff.append(", ");
                }
                buff.append(a);
            }
            buff.append(")");
        }
        return buff.toString();
    }

    public Type getType() {
        Type t = new Type();
        t.classObj = classObj;
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

    private final JavaParser context;
    private Type type;

    StringExpr(JavaParser context) {
        this.context = context;
    }

    public String toString() {
        return "STRING(\"" + javaEncode(text) + "\")";
    }

    public Type getType() {
        if (type == null) {
            type = new Type();
            type.classObj = context.getClassObj("java.lang.String");
        }
        return type;
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
    private final JavaParser context;

    VariableExpr(JavaParser context) {
        this.context = context;
    }

    public String toString() {
        init();
        StringBuilder buff = new StringBuilder();
        if ("length".equals(name) && base.getType().arrayLevel > 0) {
            buff.append("LENGTH(");
            buff.append(base.toString());
            buff.append(")");
        } else {
            if (base != null) {
                buff.append(base.toString()).append("->");
            }
            if (field != null) {
                if (field.isStatic) {
                    buff.append(JavaParser.toC(field.declaredClass + "." + field.name));
                } else {
                    buff.append(field.name);
                }
            } else {
                buff.append(JavaParser.toC(name));
            }
        }
        return buff.toString();
    }

    private void init() {
        if (field == null) {
            Type t = base.getType();
            if (t.arrayLevel > 0) {
                if ("length".equals(name)) {
                    field = new FieldObj();
                    field.type = context.getClassObj("int").baseType;
                } else {
                    throw new RuntimeException("Unknown array method: " + name);
                }
            } else {
                field = t.classObj.getField(name);
            }
        }
    }

    public Type getType() {
        init();
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

/**
 * An array initializer expression.
 */
class ArrayInitExpr implements Expr {

    Type type;
    ArrayList<Expr> list = new ArrayList<Expr>();

    public Type getType() {
        return type;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder("{ ");
        int i = 0;
        for (Expr e : list) {
            if (i++ > 0) {
                buff.append(", ");
            }
            buff.append(e.toString());
        }
        buff.append(" }");
        return buff.toString();
    }

}

/**
 * A type cast expression.
 */
class CastExpr implements Expr {

    Type type;
    Expr expr;

    public Type getType() {
        return type;
    }

    public String toString() {
        return "(" + type + ") " + expr;
    }

}

/**
 * An array access expression (get or set).
 */
class ArrayAccessExpr implements Expr {

    Expr base;
    Expr index;

    public Type getType() {
        Type t = new Type();
        t.classObj = base.getType().classObj;
        t.arrayLevel = base.getType().arrayLevel - 1;
        return t;
    }

    public String toString() {
        return base + "[" + index + "]";
    }

}
