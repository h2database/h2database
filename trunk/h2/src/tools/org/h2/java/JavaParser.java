/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

/**
 * Converts Java to C.
 */
public class JavaParser {

    private static final int TOKEN_LITERAL_CHAR = 0;
    private static final int TOKEN_LITERAL_STRING = 1;
    private static final int TOKEN_LITERAL_NUMBER = 2;
    private static final int TOKEN_RESERVED = 3;
    private static final int TOKEN_IDENTIFIER = 4;
    private static final int TOKEN_OTHER = 5;

    private static final HashSet<String> RESERVED = new HashSet<String>();
    private static final HashMap<String, ClassObj> BUILT_IN_TYPES = new HashMap<String, ClassObj>();
    private static final HashMap<String, String> JAVA_IMPORT_MAP = new HashMap<String, String>();

    private static int firstClassId;

    private String source;

    private ParseState current = new ParseState();

    private String packageName;
    private ClassObj classObj;
    private int classId = firstClassId;
    private MethodObj method;
    private FieldObj thisPointer;
    private HashMap<String, String> importMap = new HashMap<String, String>();
    private HashMap<String, ClassObj> classes = new HashMap<String, ClassObj>();
    private LinkedHashMap<String, FieldObj> localVars = new LinkedHashMap<String, FieldObj>();

    private ArrayList<Statement> nativeHeaders = new ArrayList<Statement>();

    static {
        String[] list = new String[] { "abstract", "continue", "for", "new", "switch", "assert", "default", "if",
                "package", "synchronized", "boolean", "do", "goto", "private", "this", "break", "double", "implements",
                "protected", "throw", "byte", "else", "import", "public", "throws", "case", "enum", "instanceof",
                "return", "transient", "catch", "extends", "int", "short", "try", "char", "final", "interface",
                "static", "void", "class", "finally", "long", "strictfp", "volatile", "const", "float", "native",
                "super", "while", "true", "false", "null" };
        for (String s : list) {
            RESERVED.add(s);
        }
        int id = 0;
        addBuiltInType(id++, true, "void");
        addBuiltInType(id++, true, "int");
        addBuiltInType(id++, true, "char");
        addBuiltInType(id++, true, "byte");
        addBuiltInType(id++, true, "long");
        addBuiltInType(id++, true, "double");
        addBuiltInType(id++, true, "float");
        addBuiltInType(id++, true, "boolean");
        addBuiltInType(id++, true, "short");
        String[] java = new String[] { "Boolean", "Byte", "Character", "Class", "ClassLoader", "Double", "Float",
                "Integer", "Long", "Math", "Number", "Object", "Runtime", "Short", "String", "StringBuffer",
                "StringBuilder", "System", "Thread", "ThreadGroup", "ThreadLocal", "Throwable", "Void" };
        for (String s : java) {
            JAVA_IMPORT_MAP.put(s, "java.lang." + s);
            addBuiltInType(id++, false, "java.lang." + s);
        }
        firstClassId = id;
    }

    private static void addBuiltInType(int id, boolean primitive, String type) {
        ClassObj typeObj = new ClassObj();
        typeObj.id = id;
        typeObj.name = type;
        typeObj.isPrimitive = primitive;
        BUILT_IN_TYPES.put(type, typeObj);
    }

    /**
     * Parse the source code.
     *
     * @param baseDir the base directory
     * @param className the fully qualified name of the class to parse
     */
    void parse(String baseDir, String className) {
        String fileName = baseDir + "/" + className.replace('.', '/') + ".java";
        current = new ParseState();
        try {
            RandomAccessFile file = new RandomAccessFile(fileName, "r");
            byte[] buff = new byte[(int) file.length()];
            file.readFully(buff);
            source = new String(buff, "UTF-8");
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        source = replaceUnicode(source);
        source = removeRemarks(source);
        try {
            readToken();
            parseCompilationUnit();
        } catch (Exception e) {
            throw new RuntimeException(source.substring(0, current.index) + "[*]" + source.substring(current.index), e);
        }
    }

    private String cleanPackageName(String name) {
        if (name.startsWith("org.h2.java.lang") || name.startsWith("org.h2.java.io")) {
            return name.substring("org.h2.".length());
        }
        return name;
    }

    private void parseCompilationUnit() {
        if (readIf("package")) {
            packageName = cleanPackageName(readQualifiedIdentifier());
            read(";");
        }
        while (readIf("import")) {
            String importPackageName = cleanPackageName(readQualifiedIdentifier());
            String importClass = importPackageName.substring(importPackageName.lastIndexOf('.') + 1);
            importMap.put(importClass, importPackageName);
            read(";");
        }
        while (true) {
            Statement s = readNativeStatementIf();
            if (s == null) {
                break;
            }
            nativeHeaders.add(s);
        }
        while (true) {
            classObj = new ClassObj();
            classObj.id = classId++;
            classObj.isPublic = readIf("public");
            if (readIf("class")) {
                classObj.isInterface = false;
            } else {
                read("interface");
                classObj.isInterface = true;
            }
            String name = readIdentifier();
            classObj.name = packageName == null ? "" : (packageName + ".") + name;
            // import this class
            importMap.put(name, classObj.name);
            classes.put(classObj.name, classObj);
            parseClassBody();
            if (current.token == null) {
                break;
            }
        }
    }

    private boolean isTypeOrIdentifier() {
        if (BUILT_IN_TYPES.containsKey(current.token)) {
            return true;
        }
        return current.type == TOKEN_IDENTIFIER;
    }

    private ClassObj getClass(String type) {
        ClassObj c = getClassIf(type);
        if (c == null) {
            throw new RuntimeException("Unknown type: " + type);
        }
        return c;
    }

    private ClassObj getClassIf(String type) {
        ClassObj c = BUILT_IN_TYPES.get(type);
        if (c != null) {
            return c;
        }
        c = classes.get(type);
        if (c != null) {
            return c;
        }
        String mappedType = importMap.get(type);
        if (mappedType == null) {
            mappedType = JAVA_IMPORT_MAP.get(type);
            if (mappedType == null) {
                return null;
            }
        }
        c = classes.get(mappedType);
        if (c == null) {
            c = BUILT_IN_TYPES.get(mappedType);
            if (c == null) {
                throw new RuntimeException("Unknown class: " + mappedType);
            }
        }
        return c;
    }

    private void parseClassBody() {
        read("{");
        localVars.clear();
        while (true) {
            if (readIf("}")) {
                break;
            }
            thisPointer = null;
            while (true) {
                Statement s = readNativeStatementIf();
                if (s == null) {
                    break;
                }
                classObj.nativeCode.add(s);
            }
            thisPointer = null;
            boolean isStatic = false;
            boolean isFinal = false;
            boolean isPrivate = false;
            boolean isPublic = false;
            boolean isNative = false;
            while (true) {
                if (readIf("static")) {
                    isStatic = true;
                } else if (readIf("final")) {
                    isFinal = true;
                } else if (readIf("native")) {
                    isNative = true;
                } else if (readIf("private")) {
                    isPrivate = true;
                } else if (readIf("public")) {
                    isPublic = true;
                } else {
                    break;
                }
            }
            if (readIf("{")) {
                method = new MethodObj();
                method.name = isStatic ? "cl_init_obj" : "init_obj";
                method.isStatic = isStatic;
                localVars.clear();
                if (!isStatic) {
                    initThisPointer();
                }
                method.block = readStatement();
                classObj.addMethod(method);
            } else {
                String typeName = readTypeOrIdentifier();
                Type type = readType(typeName);
                method = new MethodObj();
                method.returnType = type;
                method.isStatic = isStatic;
                method.isFinal = isFinal;
                method.isPublic = isPublic;
                method.isPrivate = isPrivate;
                method.isNative = isNative;
                localVars.clear();
                if (!isStatic) {
                    initThisPointer();
                }
                if (readIf("(")) {
                    if (type.type != classObj) {
                        throw getSyntaxException("Constructor of wrong type: " + type);
                    }
                    method.name = "init_obj";
                    method.isConstructor = true;
                    parseFormalParameters();
                    if (!readIf(";")) {
                        method.block = readStatement();
                    }
                    classObj.addMethod(method);
                } else {
                    String name = readIdentifier();
                    method.name = name;
                    if (readIf("(")) {
                        parseFormalParameters();
                        if (!readIf(";")) {
                            method.block = readStatement();
                        }
                        classObj.addMethod(method);
                    } else {
                        FieldObj field = new FieldObj();
                        field.type = type;
                        field.name = name;
                        field.isStatic = isStatic;
                        field.isFinal = isFinal;
                        field.isPublic = isPublic;
                        field.isPrivate = isPrivate;
                        if (isStatic) {
                            classObj.addStaticField(field);
                        } else {
                            classObj.addInstanceField(field);
                        }
                        if (readIf("=")) {
                            field.value = readExpr();
                        }
                        read(";");
                    }
                }
            }
        }
    }

    private void initThisPointer() {
        thisPointer = new FieldObj();
        thisPointer.isLocal = true;
        thisPointer.name = "this";
        thisPointer.type = new Type();
        thisPointer.type.type = classObj;
    }

    private Type readType(String name) {
        Type type = new Type();
        type.type = getClass(name);
        while (readIf("[")) {
            read("]");
            type.arrayLevel++;
        }
        if (readIf("...")) {
            type.arrayLevel++;
        }
        return type;
    }

    private void parseFormalParameters() {
        if (readIf(")")) {
            return;
        }
        while (true) {
            FieldObj field = new FieldObj();
            field.isLocal = true;
            String typeName = readTypeOrIdentifier();
            field.type = readType(typeName);
            field.name = readIdentifier();
            method.parameters.put(field.name, field);
            if (readIf(")")) {
                break;
            }
            read(",");
        }
    }

    private String readTypeOrIdentifier() {
        if (current.type == TOKEN_RESERVED) {
            if (BUILT_IN_TYPES.containsKey(current.token)) {
                return read();
            }
        }
        String s = readIdentifier();
        while (readIf(".")) {
            s += "." + readIdentifier();
        }
        return s;
    }

    private Statement readNativeStatementIf() {
        if (readIf("//")) {
            read();
            int start = current.index;
            while (source.charAt(current.index) != '\n') {
                current.index++;
            }
            StatementNative stat = new StatementNative();
            stat.code = source.substring(start, current.index).trim();
            read();
            return stat;
        } else if (readIf("/*")) {
            read();
            int start = current.index;
            while (source.charAt(current.index) != '*' || source.charAt(current.index + 1) != '/') {
                current.index++;
            }
            StatementNative stat = new StatementNative();
            stat.code = source.substring(start, current.index).trim();
            current.index += 2;
            read();
            return stat;
        }
        return null;
    }

    private Statement readStatement() {
        Statement s = readNativeStatementIf();
        if (s != null) {
            return s;
        }
        if (readIf(";")) {
            return new EmptyStatement();
        } else if (readIf("{")) {
            StatementBlock stat = new StatementBlock();
            while (true) {
                stat.instructions.add(readStatement());
                if (readIf("}")) {
                    break;
                }
            }
            return stat;
        } else if (readIf("if")) {
            IfStatement ifStat = new IfStatement();
            read("(");
            ifStat.condition = readExpr();
            read(")");
            ifStat.block = readStatement();
            if (readIf("else")) {
                ifStat.elseBlock = readStatement();
            }
            return ifStat;
        } else if (readIf("while")) {
            WhileStatement whileStat = new WhileStatement();
            read("(");
            whileStat.condition = readExpr();
            read(")");
            whileStat.block = readStatement();
            return whileStat;
        } else if (readIf("break")) {
            read(";");
            return new BreakStatement();
        } else if (readIf("continue")) {
            read(";");
            return new ContinueStatement();
        } else if (readIf("switch")) {
            SwitchStatement switchStat = new SwitchStatement();
            read("(");
            switchStat.expr = readExpr();
            read(")");
            read("{");
            while (true) {
                if (readIf("default")) {
                    read(":");
                    StatementBlock block = new StatementBlock();
                    switchStat.defaultBlock = block;
                    while (true) {
                        block.instructions.add(readStatement());
                        if (current.token.equals("case") || current.token.equals("default") || current.token.equals("}")) {
                            break;
                        }
                    }
                } else if (readIf("case")) {
                    switchStat.cases.add(readExpr());
                    read(":");
                    StatementBlock block = new StatementBlock();
                    while (true) {
                        block.instructions.add(readStatement());
                        if (current.token.equals("case") || current.token.equals("default") || current.token.equals("}")) {
                            break;
                        }
                    }
                    switchStat.blocks.add(block);
                } else if (readIf("}")) {
                    break;
                }
            }
            return switchStat;
        } else if (readIf("for")) {
            ForStatement forStat = new ForStatement();
            read("(");
            ParseState back = copyParseState();
            try {
                String typeName = readTypeOrIdentifier();
                Type type = readType(typeName);
                String name = readIdentifier();
                read(":");
                forStat.iterableType = type;
                forStat.iterableVariable = name;
                forStat.iterable = readExpr();
            } catch (Exception e) {
                current = back;
                forStat.init = readStatement();
                forStat.condition = readExpr();
                read(";");
                do {
                    forStat.updates.add(readExpr());
                } while (readIf(","));
            }
            read(")");
            forStat.block = readStatement();
            return forStat;
        } else if (readIf("do")) {
            DoWhileStatement doWhileStat = new DoWhileStatement();
            doWhileStat.block = readStatement();
            read("while");
            read("(");
            doWhileStat.condition = readExpr();
            read(")");
            read(";");
            return doWhileStat;
        } else if (readIf("return")) {
            ReturnStatement returnStat = new ReturnStatement();
            if (!readIf(";")) {
                returnStat.expr = readExpr();
                read(";");
            }
            return returnStat;
        } else {
            if (isTypeOrIdentifier()) {
                ParseState start = copyParseState();
                String name = readTypeOrIdentifier();
                ClassObj c = getClassIf(name);
                if (c != null) {
                    VarDecStatement dec = new VarDecStatement();
                    dec.type = readType(name);
                    while (true) {
                        String varName = readIdentifier();
                        Expr value = null;
                        if (readIf("=")) {
                            value = readExpr();
                        }
                        FieldObj f = new FieldObj();
                        f.isLocal = true;
                        f.type = dec.type;
                        f.name = varName;
                        localVars.put(varName, f);
                        dec.variables.add(varName);
                        dec.values.add(value);
                        if (readIf(";")) {
                            break;
                        }
                        read(",");
                    }
                    return dec;
                }
                current = start;
                // ExprStatement
            }
            ExprStatement stat = new ExprStatement();
            stat.expr = readExpr();
            read(";");
            return stat;
        }
    }

    private ParseState copyParseState() {
        ParseState state = new ParseState();
        state.index = current.index;
        state.line = current.line;
        state.token = current.token;
        state.type = current.type;
        return state;
    }

    private Expr readExpr() {
        Expr expr = readExpr1();
        String assign = current.token;
        if (readIf("=") || readIf("+=") || readIf("-=") || readIf("*=") || readIf("/=") || readIf("&=") || readIf("|=")
                || readIf("^=") || readIf("%=") || readIf("<<=") || readIf(">>=") || readIf(">>>=")) {
            AssignExpr assignOp = new AssignExpr();
            assignOp.left = expr;
            assignOp.op = assign;
            assignOp.right = readExpr1();
            expr = assignOp;
        }
        return expr;
    }

    private Expr readExpr1() {
        Expr expr = readExpr2();
        if (readIf("?")) {
            ConditionalExpr ce = new ConditionalExpr();
            ce.condition = expr;
            ce.ifTrue = readExpr();
            read(":");
            ce.ifFalse = readExpr();
            return ce;
        }
        return expr;
    }

    private Expr readExpr2() {
        Expr expr = readExpr3();
        while (true) {
            String infixOp = current.token;
            if (readIf("||") || readIf("&&") || readIf("|") || readIf("^") || readIf("&") || readIf("==") || readIf("!=")
                    || readIf("<") || readIf(">") || readIf("<=") || readIf(">=") || readIf("<<") || readIf(">>")
                    || readIf(">>>") || readIf("+") || readIf("-") || readIf("*") || readIf("/") || readIf("%")) {
                OpExpr opExpr = new OpExpr();
                opExpr.left = expr;
                opExpr.op = infixOp;
                opExpr.right = readExpr3();
                expr = opExpr;
            } else {
                break;
            }
        }
        return expr;
    }

    private Expr readExpr3() {
        if (readIf("(")) {
            Expr expr = readExpr();
            read(")");
            return expr;
        }
        String prefix = current.token;
        if (readIf("++") || readIf("--") || readIf("!") || readIf("~") || readIf("+") || readIf("-")) {
            OpExpr expr = new OpExpr();
            expr.op = prefix;
            expr.right = readExpr3();
            return expr;
        }
        Expr expr = readExpr4();
        String suffix = current.token;
        if (readIf("++") || readIf("--")) {
            OpExpr opExpr = new OpExpr();
            opExpr.left = expr;
            opExpr.op = suffix;
            expr = opExpr;
        }
        return expr;
    }

    private Expr readExpr4() {
        if (readIf("false")) {
            LiteralExpr expr = new LiteralExpr();
            expr.literal = "false";
            return expr;
        }
        if (readIf("true")) {
            LiteralExpr expr = new LiteralExpr();
            expr.literal = "true";
            return expr;
        }
        if (readIf("null")) {
            LiteralExpr expr = new LiteralExpr();
            expr.literal = "null";
            return expr;
        }
        if (current.type == TOKEN_LITERAL_NUMBER) {
            LiteralExpr expr = new LiteralExpr();
            expr.literal = current.token.substring(1);
            readToken();
            return expr;
        }
        Expr expr;
        expr = readExpr5();
        Type t = expr.getType();
        while (true) {
            if (readIf(".")) {
                String n = readIdentifier();
                if (readIf("(")) {
                    CallExpr e2 = new CallExpr(this, expr, null, n, false);
                    if (!readIf(")")) {
                        while (true) {
                            e2.args.add(readExpr());
                            if (!readIf(",")) {
                                read(")");
                                break;
                            }
                        }
                    }
                    expr = e2;
                } else {
                    VariableExpr e2 = new VariableExpr();
                    e2.base = expr;
                    expr = e2;
                    if (n.equals("length") && t.arrayLevel > 0) {
                        e2.field = new FieldObj();
                        e2.field.name = "length";
                    } else {
                        if (t == null || t.type == null) {
                            e2.name = n;
                        } else {
                            FieldObj f = t.type.instanceFields.get(n);
                            if (f == null) {
                                throw getSyntaxException("Unknown field: " + expr + "." + n);
                            }
                            e2.field = f;
                        }
                    }
                }
            } else if (readIf("[")) {
                if (t.arrayLevel == 0) {
                    throw getSyntaxException("Not an array: " + expr);
                }
                Expr arrayIndex = readExpr();
                read("]");
                // TODO arrayGet or arraySet
                return arrayIndex;
            } else {
                break;
            }
        }
        return expr;
    }

    private Expr readExpr5() {
        if (readIf("new")) {
            NewExpr expr = new NewExpr();
            String typeName = readTypeOrIdentifier();
            expr.type = getClass(typeName);
            if (readIf("(")) {
                read(")");
            } else {
                while (readIf("[")) {
                    expr.arrayInitExpr.add(readExpr());
                    read("]");
                }
            }
            return expr;
        }
        if (current.type == TOKEN_LITERAL_STRING) {
            StringExpr expr = new StringExpr();
            expr.text = current.token.substring(1);
            readToken();
            return expr;
        }
        if (readIf("this")) {
            VariableExpr expr = new VariableExpr();
            expr.field = thisPointer;
            if (thisPointer == null) {
                throw getSyntaxException("this usage in static context");
            }
            return expr;
        }
        String name = readIdentifier();
        if (readIf("(")) {
            VariableExpr t = new VariableExpr();
            t.field = thisPointer;
            CallExpr expr = new CallExpr(this, t, classObj.name, name, false);
            if (!readIf(")")) {
                while (true) {
                    expr.args.add(readExpr());
                    if (!readIf(",")) {
                        read(")");
                        break;
                    }
                }
            }
            return expr;
        }
        VariableExpr expr = new VariableExpr();
        FieldObj f = localVars.get(name);
        if (f == null) {
            f = method.parameters.get(name);
        }
        if (f == null) {
            f = classObj.staticFields.get(name);
        }
        if (f == null) {
            f = classObj.instanceFields.get(name);
        }
        if (f == null) {
            String imp = importMap.get(name);
            if (imp == null) {
                imp = JAVA_IMPORT_MAP.get(name);
            }
            if (imp != null) {
                name = imp;
                if (readIf(".")) {
                    String n = readIdentifier();
                    if (readIf("(")) {
                        CallExpr e2 = new CallExpr(this, null, imp, n, true);
                        if (!readIf(")")) {
                            while (true) {
                                e2.args.add(readExpr());
                                if (!readIf(",")) {
                                    read(")");
                                    break;
                                }
                            }
                        }
                        return e2;
                    }
                    VariableExpr e2 = new VariableExpr();
                    // static member variable
                    e2.name = imp + "." + n;
                    ClassObj c = classes.get(imp);
                    FieldObj sf = c.staticFields.get(n);
                    e2.field = sf;
                    return e2;
                }
                // TODO static field or method of a class
            }
        }
        expr.field = f;
        if (f != null && (!f.isLocal && !f.isStatic)) {
            VariableExpr ve = new VariableExpr();
            ve.field = thisPointer;
            expr.base = ve;
            if (thisPointer == null) {
                throw getSyntaxException("this usage in static context");
            }
        }
        expr.name = name;
        return expr;
    }

    private void read(String string) {
        if (!readIf(string)) {
            throw getSyntaxException(string + " expected, got " + current.token);
        }
    }

    private String readQualifiedIdentifier() {
        String id = readIdentifier();
        if (localVars.containsKey(id)) {
            return id;
        }
        if (classObj != null) {
            if (classObj.staticFields.containsKey(id)) {
                return id;
            }
            if (classObj.instanceFields.containsKey(id)) {
                return id;
            }
        }
        String fullName = importMap.get(id);
        if (fullName != null) {
            return fullName;
        }
        while (readIf(".")) {
            id += "." + readIdentifier();
        }
        return id;
    }

    private String readIdentifier() {
        if (current.type != TOKEN_IDENTIFIER) {
            throw getSyntaxException("identifier expected, got " + current.token);
        }
        String result = current.token;
        readToken();
        return result;
    }

    private boolean readIf(String token) {
        if (current.type != TOKEN_IDENTIFIER && token.equals(current.token)) {
            readToken();
            return true;
        }
        return false;
    }

    private String read() {
        String token = current.token;
        readToken();
        return token;
    }

    private RuntimeException getSyntaxException(String message) {
        return new RuntimeException(message, new ParseException(source, current.index));
    }

    /**
     * Replace all Unicode escapes.
     *
     * @param s the text
     * @return the cleaned text
     */
    static String replaceUnicode(String s) {
        if (s.indexOf("\\u") < 0) {
            return s;
        }
        StringBuilder buff = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            if (s.substring(i).startsWith("\\\\")) {
                buff.append("\\\\");
                i++;
            } else if (s.substring(i).startsWith("\\u")) {
                i += 2;
                while (s.charAt(i) == 'u') {
                    i++;
                }
                String c = s.substring(i, i + 4);
                buff.append((char) Integer.parseInt(c, 16));
                i += 4;
            } else {
                buff.append(s.charAt(i));
            }
        }
        return buff.toString();
    }

    /**
     * Replace all Unicode escapes and remove all remarks.
     *
     * @param s the source code
     * @return the cleaned source code
     */
    static String removeRemarks(String s) {
        char[] chars = s.toCharArray();
        for (int i = 0; i >= 0 && i < s.length(); i++) {
            if (s.charAt(i) == '\'') {
                i++;
                while (true) {
                    if (s.charAt(i) == '\\') {
                        i++;
                    } else if (s.charAt(i) == '\'') {
                        break;
                    }
                    i++;
                }
                continue;
            } else if (s.charAt(i) == '\"') {
                i++;
                while (true) {
                    if (s.charAt(i) == '\\') {
                        i++;
                    } else if (s.charAt(i) == '\"') {
                        break;
                    }
                    i++;
                }
                continue;
            }
            String sub = s.substring(i);
            if (sub.startsWith("/*") && !sub.startsWith("/* c:")) {
                int j = i;
                i = s.indexOf("*/", i + 2) + 2;
                for (; j < i; j++) {
                    if (chars[j] > ' ') {
                        chars[j] = ' ';
                    }
                }
            } else if (sub.startsWith("//") && !sub.startsWith("// c:")) {
                int j = i;
                i = s.indexOf('\n', i);
                while (j < i) {
                    chars[j++] = ' ';
                }
            }
        }
        return new String(chars) + "  ";
    }

    private void readToken() {
        int ch;
        while (true) {
            if (current.index >= source.length()) {
                current.token = null;
                return;
            }
            ch = source.charAt(current.index);
            if (ch == '\n') {
                current.line++;
            } else if (ch > ' ') {
                break;
            }
            current.index++;
        }
        int start = current.index;
        if (Character.isJavaIdentifierStart(ch)) {
            while (Character.isJavaIdentifierPart(source.charAt(current.index))) {
                current.index++;
            }
            current.token = source.substring(start, current.index);
            if (RESERVED.contains(current.token)) {
                current.type = TOKEN_RESERVED;
            } else {
                current.type = TOKEN_IDENTIFIER;
            }
            return;
        } else if (Character.isDigit(ch) || (ch == '.' && Character.isDigit(source.charAt(current.index + 1)))) {
            String s = source.substring(current.index);
            current.token = "0" + readNumber(s);
            current.index += current.token.length() - 1;
            current.type = TOKEN_LITERAL_NUMBER;
            return;
        }
        current.index++;
        switch (ch) {
        case '\'': {
            while (true) {
                if (source.charAt(current.index) == '\\') {
                    current.index++;
                } else if (source.charAt(current.index) == '\'') {
                    break;
                }
                current.index++;
            }
            current.index++;
            current.token = source.substring(start + 1, current.index);
            current.token = "\'" + javaDecode(current.token, '\'');
            current.type = TOKEN_LITERAL_CHAR;
            return;
        }
        case '\"': {
            while (true) {
                if (source.charAt(current.index) == '\\') {
                    current.index++;
                } else if (source.charAt(current.index) == '\"') {
                    break;
                }
                current.index++;
            }
            current.index++;
            current.token = source.substring(start + 1, current.index);
            current.token = "\"" + javaDecode(current.token, '\"');
            current.type = TOKEN_LITERAL_STRING;
            return;
        }
        case '(':
        case ')':
        case '[':
        case ']':
        case '{':
        case '}':
        case ';':
        case ',':
        case '?':
        case ':':
            break;
        case '.':
            if (source.charAt(current.index) == '.' && source.charAt(current.index + 1) == '.') {
                current.index += 2;
           }
            break;
        case '+':
            if (source.charAt(current.index) == '=' || source.charAt(current.index) == '+') {
                current.index++;
            }
            break;
        case '-':
            if (source.charAt(current.index) == '=' || source.charAt(current.index) == '-') {
                current.index++;
            }
            break;
        case '>':
            if (source.charAt(current.index) == '>') {
                current.index++;
                if (source.charAt(current.index) == '>') {
                    current.index++;
                }
            }
            if (source.charAt(current.index) == '=') {
                current.index++;
            }
            break;
        case '<':
            if (source.charAt(current.index) == '<') {
                current.index++;
            }
            if (source.charAt(current.index) == '=') {
                current.index++;
            }
            break;
        case '/':
            if (source.charAt(current.index) == '*' || source.charAt(current.index) == '/') {
                current.index++;
            }
            break;
        case '*':
        case '~':
        case '!':
        case '=':
        case '%':
        case '^':
            if (source.charAt(current.index) == '=') {
                current.index++;
            }
            break;
        case '&':
            if (source.charAt(current.index) == '&') {
                current.index++;
            } else if (source.charAt(current.index) == '=') {
                current.index++;
            }
            break;
        case '|':
            if (source.charAt(current.index) == '|') {
                current.index++;
            } else if (source.charAt(current.index) == '=') {
                current.index++;
            }
            break;
        }
        current.type = TOKEN_OTHER;
        current.token = source.substring(start, current.index);
    }

    /**
     * Parse a number literal and returns it.
     *
     * @param s the source code
     * @return the number
     */
    static String readNumber(String s) {
        int i = 0;
        if (s.startsWith("0x") || s.startsWith("0X")) {
            i = 2;
            while (true) {
                char ch = s.charAt(i);
                if ((ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') && (ch < 'A' || ch > 'F')) {
                    break;
                }
                i++;
            }
            if (s.charAt(i) == 'l' || s.charAt(i) == 'L') {
                i++;
            }
        } else {
            while (true) {
                char ch = s.charAt(i);
                if ((ch < '0' || ch > '9') && ch != '.') {
                    break;
                }
                i++;
            }
            if (s.charAt(i) == 'e' || s.charAt(i) == 'E') {
                i++;
                if (s.charAt(i) == '-' || s.charAt(i) == '+') {
                    i++;
                }
                while (Character.isDigit(s.charAt(i))) {
                    i++;
                }
            }
            if (s.charAt(i) == 'f' || s.charAt(i) == 'F' || s.charAt(i) == 'd'
                    || s.charAt(i) == 'D' || s.charAt(i) == 'L' || s.charAt(i) == 'l') {
                i++;
            }
        }
        return s.substring(0, i);
    }

    private static RuntimeException getFormatException(String s, int i) {
        return new RuntimeException(new ParseException(s, i));
    }

    private static String javaDecode(String s, char end) {
        StringBuilder buff = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == end) {
                break;
            } else if (c == '\\') {
                if (i >= s.length()) {
                    throw getFormatException(s, s.length() - 1);
                }
                c = s.charAt(++i);
                switch (c) {
                case 't':
                    buff.append('\t');
                    break;
                case 'r':
                    buff.append('\r');
                    break;
                case 'n':
                    buff.append('\n');
                    break;
                case 'b':
                    buff.append('\b');
                    break;
                case 'f':
                    buff.append('\f');
                    break;
                case '"':
                    buff.append('"');
                    break;
                case '\'':
                    buff.append('\'');
                    break;
                case '\\':
                    buff.append('\\');
                    break;
                case 'u': {
                    try {
                        c = (char) (Integer.parseInt(s.substring(i + 1, i + 5), 16));
                    } catch (NumberFormatException e) {
                        throw getFormatException(s, i);
                    }
                    i += 4;
                    buff.append(c);
                    break;
                }
                default:
                    if (c >= '0' && c <= '9') {
                        try {
                            c = (char) (Integer.parseInt(s.substring(i, i + 3), 8));
                        } catch (NumberFormatException e) {
                            throw getFormatException(s, i);
                        }
                        i += 2;
                        buff.append(c);
                    } else {
                        throw getFormatException(s, i);
                    }
                }
            } else {
                buff.append(c);
            }
        }
        return buff.toString();
    }

    /**
     * Write the C header.
     *
     * @param out the output writer
     */
    void writeHeader(PrintWriter out) {
        for (Statement s : nativeHeaders) {
            out.println(s);
        }
        out.println();
        for (ClassObj c : classes.values()) {
            out.println("/* " + c.name + ".h */");
            for (FieldObj f : c.staticFields.values()) {
                if (f.isFinal) {
                    out.println("#define " + toC(c.name + "." + f.name) + " (" + f.value + ")");
                } else {
                    out.print("extern " + toC(f.type.toString()) + " " + toC(c.name + "." + f.name));
                    out.println(";");
                }
            }
            out.println("struct " + toC(c.name) + " {");
            for (FieldObj f : c.instanceFields.values()) {
                out.print("    " + toC(f.type.toString()) + " " + f.name);
                if (f.value != null) {
                    out.print(" = " + f.value);
                }
                out.println(";");
            }
            if (c.instanceFields.size() == 0) {
                out.println("int dummy;");
            }
            out.println("};");
            out.println("typedef struct " + toC(c.name) + " " + toC(c.name) + ";");
            for (MethodObj m : c.methods.values()) {
                out.print(m.returnType + " " + toC(c.name) + "_" + m.name + "(");
                int i = 0;
                if (!m.isStatic && !m.isConstructor) {
                    out.print(toC(c.name) + "* this");
                    i++;
                }
                for (FieldObj p : m.parameters.values()) {
                    if (i > 0) {
                        out.print(", ");
                    }
                    out.print(p.type + " " + p.name);
                    i++;
                }
                out.println(");");
            }
            out.println();
        }
    }

    /**
     * Write the C source code.
     *
     * @param out the output writer
     */
    void writeSource(PrintWriter out) {
        for (ClassObj c : classes.values()) {
            out.println("/* " + c.name + ".c */");
            for (Statement s : c.nativeCode) {
                out.println(s);
            }
            for (FieldObj f : c.staticFields.values()) {
                if (!f.isFinal) {
                    out.print(toC(f.type.toString()) + " " + toC(c.name + "." + f.name));
                    if (f.value != null) {
                        out.print(" = " + f.value);
                    }
                    out.println(";");
                }
            }
            for (MethodObj m : c.methods.values()) {
                out.print(m.returnType + " " + toC(c.name) + "_" + m.name + "(");
                int i = 0;
                if (!m.isStatic && !m.isConstructor) {
                    out.print(toC(c.name) + "* this");
                    i++;
                }
                for (FieldObj p : m.parameters.values()) {
                    if (i > 0) {
                        out.print(", ");
                    }
                    out.print(p.type + " " + p.name);
                    i++;
                }
                out.println(") {");
                if (m.isConstructor) {
                    out.println(indent(toC(c.name) + "* this = NEW_OBJ(" + c.id + ", " + toC(c.name) +");"));
                }
                if (m.block != null) {
                    out.print(m.block.toString());
                }
                out.println("}");
                out.println();
            }
        }
    }

    private static String indent(String s, int spaces) {
        StringBuilder buff = new StringBuilder(s.length() + spaces);
        for (int i = 0; i < s.length();) {
            for (int j = 0; j < spaces; j++) {
                buff.append(' ');
            }
            int n = s.indexOf('\n', i);
            n = n < 0 ? s.length() : n + 1;
            buff.append(s.substring(i, n));
            i = n;
        }
        if (!s.endsWith("\n")) {
            buff.append('\n');
        }
        return buff.toString();
    }

    /**
     * Move the source code 4 levels to the right.
     *
     * @param o the source code
     * @return the indented code
     */
    static String indent(String o) {
        return indent(o, 4);
    }

    /**
     * Get the C representation of this identifier.
     *
     * @param identifier the identifier
     * @return the C representation
     */
    static String toC(String identifier) {
        return identifier.replace('.', '_');
    }

    ClassObj getClassObj() {
        return classObj;
    }

}

/**
 * The parse state.
 */
class ParseState {

    /**
     * The parse index.
     */
    int index;

    /**
     * The token type
     */
    int type;

    /**
     * The token text.
     */
    String token;

    /**
     * The line number.
     */
    int line;
}