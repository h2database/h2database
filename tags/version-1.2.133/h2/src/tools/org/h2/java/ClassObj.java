/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java;

import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * A class or interface.
 */
public class ClassObj {

    /**
     * The super class (null for java.lang.Object or primitive types).
     */
    String superClassName;

    /**
     * The list of interfaces that this class implements.
     */
    ArrayList<String> interfaceNames = new ArrayList<String>();


    /**
     * The fully qualified class name.
     */
    String className;

    /**
     * Whether this is an interface.
     */
    boolean isInterface;

    /**
     * Whether this class is public.
     */
    boolean isPublic;

    /**
     * Whether this is a primitive class (int, char,...)
     */
    boolean isPrimitive;

    /**
     * The primitive type (higher types are more complex)
     */
    int primitiveType;

    /**
     * The imported classes.
     */
    ArrayList<ClassObj> imports = new ArrayList<ClassObj>();

    /**
     * The per-instance fields.
     */
    LinkedHashMap<String, FieldObj> instanceFields = new LinkedHashMap<String, FieldObj>();

    /**
     * The static fields of this class.
     */
    LinkedHashMap<String, FieldObj> staticFields = new LinkedHashMap<String, FieldObj>();

    /**
     * The methods.
     */
    LinkedHashMap<String, ArrayList<MethodObj>> methods = new LinkedHashMap<String, ArrayList<MethodObj>>();

    /**
     * The list of native statements.
     */
    ArrayList<Statement> nativeCode = new ArrayList<Statement>();

    /**
     * The class number.
     */
    int id;

    /**
     * Get the base type of this class.
     */
    Type baseType;

    ClassObj() {
        baseType = new Type();
        baseType.classObj = this;
    }

    /**
     * Add a method.
     *
     * @param method the method
     */
    void addMethod(MethodObj method) {
        ArrayList<MethodObj> list = methods.get(method.name);
        if (list == null) {
            list = new ArrayList<MethodObj>();
            methods.put(method.name, list);
        } else {
            method.name = method.name + "_" + (list.size() + 1);
        }
        list.add(method);
    }

    /**
     * Add an instance field.
     *
     * @param field the field
     */
    void addInstanceField(FieldObj field) {
        instanceFields.put(field.name, field);
    }

    /**
     * Add a static field.
     *
     * @param field the field
     */
    void addStaticField(FieldObj field) {
        staticFields.put(field.name, field);
    }

    public String toString() {
        if (isPrimitive) {
            return "j" + className;
        }
        return className;
    }

    /**
     * Get the method.
     *
     * @param find the method name in the source code
     * @param args the parameters
     * @return the method
     */
    MethodObj getMethod(String find, ArrayList<Expr> args) {
        ArrayList<MethodObj> list = methods.get(find);
        if (list == null) {
            throw new RuntimeException("Method not found: " + className + " " + find);
        }
        if (list.size() == 1) {
            return list.get(0);
        }
        for (MethodObj m : list) {
            if (!m.isVarArgs && m.parameters.size() != args.size()) {
                continue;
            }
            boolean match = true;
            int i = 0;
            for (FieldObj f : m.parameters.values()) {
                Expr a = args.get(i++);
                Type t = a.getType();
                if (!t.equals(f.type)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return m;
            }
        }
        throw new RuntimeException("Method not found: " + className);
    }

    /**
     * Get the field with the given name.
     *
     * @param name the field name
     * @return the field
     */
    FieldObj getField(String name) {
        return instanceFields.get(name);
    }

}

/**
 * A method.
 */
class MethodObj {

    /**
     * Whether the last parameter is a var args parameter.
     */
    boolean isVarArgs;

    /**
     * Whether this method is static.
     */
    boolean isStatic;

    /**
     * Whether this method is private.
     */
    boolean isPrivate;

    /**
     * Whether this method is overridden.
     */
    boolean isVirtual;

    /**
     * The name.
     */
    String name;

    /**
     * The statement block (if any).
     */
    Statement block;

    /**
     * The return type.
     */
    Type returnType;

    /**
     * The parameter list.
     */
    LinkedHashMap<String, FieldObj> parameters = new LinkedHashMap<String, FieldObj>();

    /**
     * Whether this method is final.
     */
    boolean isFinal;

    /**
     * Whether this method is public.
     */
    boolean isPublic;

    /**
     * Whether this method is native.
     */
    boolean isNative;

    /**
     * Whether this is a constructor.
     */
    boolean isConstructor;
}

/**
 * A field.
 */
class FieldObj {

    /**
     * The type.
     */
    Type type;

    /**
     * Whether this is a local field.
     */
    boolean isLocal;

    /**
     * The field name.
     */
    String name;

    /**
     * Whether this field is static.
     */
    boolean isStatic;

    /**
     * Whether this field is final.
     */
    boolean isFinal;

    /**
     * Whether this field is private.
     */
    boolean isPrivate;

    /**
     * Whether this field is public.
     */
    boolean isPublic;

    /**
     * The initial value expression (may be null).
     */
    Expr value;

    /**
     * The class where this field is declared.
     */
    ClassObj declaredClass;

}

/**
 * A type.
 */
class Type {

    /**
     * The class.
     */
    ClassObj classObj;

    /**
     * The array nesting level. 0 if not an array.
     */
    int arrayLevel;

    /**
     * Whether this is a var args parameter.
     */
    boolean isVarArgs;

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append(JavaParser.toC(classObj.toString()));
        if (!classObj.isPrimitive) {
            buff.append("*");
        }
        for (int i = 0; i < arrayLevel; i++) {
            buff.append("*");
        }
        return buff.toString();
    }

    boolean isObject() {
        return arrayLevel > 0 || !classObj.isPrimitive;
    }

    public int hashCode() {
        return toString().hashCode();
    }

    public boolean equals(Object other) {
        if (other instanceof Type) {
            return this.toString().equals(other.toString());
        }
        return false;
    }

}

