/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Stack;

/**
 * This class converts a method to a SQL expression by interpreting
 * (decompiling) the bytecode of the class.
 */
public class ClassReader {

    private static final boolean DEBUG = false;

    private byte[] data;
    private int pos;
    private int[] cpType;
    private String[] cpString;
    private int[] cpInt;
    private int startByteCode;
    private String methodName;

    private String convertMethodName;
    private String result;
    private Stack<String> stack = new Stack<String>();
    private ArrayList<String> variables = new ArrayList<String>();
    private boolean end;
    private boolean condition;
    private int nextPc;

    private void debug(String s) {
        if (DEBUG) {
            System.out.println(s);
        }
    }

    public String decompile(Object instance, String methodName) {
        this.convertMethodName = methodName;
        Class< ? > clazz = instance.getClass();
        String className = clazz.getName();
        debug("class name " + className);
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        try {
            InputStream in = clazz.getClassLoader().getResource(className.replace('.', '/') + ".class").openStream();
            while (true) {
                int x = in.read();
                if (x < 0) {
                    break;
                }
                buff.write(x);
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not read class bytecode", e);
        }
        data = buff.toByteArray();
        int header = readInt();
        debug("header: " + Integer.toHexString(header));
        int minorVersion = readShort();
        int majorVersion = readShort();
        debug("version: " + majorVersion + "." + minorVersion);
        int constantPoolCount = readShort();
        cpString = new String[constantPoolCount];
        cpInt = new int[constantPoolCount];
        cpType = new int[constantPoolCount];
        for (int i = 1; i < constantPoolCount; i++) {
            int tag = readByte();
            cpType[i] = tag;
            switch(tag) {
            case 1:
                cpString[i] = readString();
                break;
            case 3: {
                int x = readInt();
                cpString[i] = String.valueOf(x);
                break;
            }
            case 4: {
                int x = readInt();
                cpString[i] = Float.toString(Float.intBitsToFloat(x));
                cpInt[i] = x;
                break;
            }
            case 5: {
                long x = readLong();
                cpString[i] = String.valueOf(x);
                i++;
                break;
            }
            case 6: {
                long x = readLong();
                cpString[i] = String.valueOf(Double.longBitsToDouble(x));
                i++;
                break;
            }
            case 7: {
                int x = readShort();
                cpString[i] = "class";
                cpInt[i] = x;
                break;
            }
            case 8: {
                int x = readShort();
                cpString[i] = "string";
                cpInt[i] = x;
                break;
            }
            case 9: {
                int x = readInt();
                cpString[i] = "field";
                cpInt[i] = x;
                break;
            }
            case 10: {
                int x = readInt();
                cpString[i] = "method";
                cpInt[i] = x;
                break;
            }
            case 11: {
                int x = readInt();
                cpString[i] = "interface method";
                cpInt[i] = x;
                break;
            }
            case 12: {
                int x = readInt();
                cpString[i] = "name and type";
                cpInt[i] = x;
                break;
            }
            default:
                throw new Error("Unsupported constant pool tag: " + tag);
            }
        }
        int accessFlags = readShort();
        debug("access flags: " + accessFlags);
        int classRef = readShort();
        debug("class: " + cpString[cpInt[classRef]]);
        int superClassRef = readShort();
        debug(" extends " + cpString[cpInt[superClassRef]]);
        int interfaceCount = readShort();
        for (int i = 0; i < interfaceCount; i++) {
            int interfaceRef = readShort();
            debug(" implements " + cpString[cpInt[interfaceRef]]);
        }
        int fieldCount = readShort();
        for (int i = 0; i < fieldCount; i++) {
            readField();
        }
        int methodCount = readShort();
        for (int i = 0; i < methodCount; i++) {
            readMethod();
        }
        readAttributes();
        return result;
    }

    private void readField() {
        int accessFlags = readShort();
        int nameIndex = readShort();
        int descIndex = readShort();
        debug("    " + cpString[descIndex] + " " + cpString[nameIndex] + " " + accessFlags);
        readAttributes();
    }

    private void readMethod() {
        int accessFlags = readShort();
        int nameIndex = readShort();
        int descIndex = readShort();
        String desc = cpString[descIndex];
        methodName = cpString[nameIndex];
        debug("    " + desc + " " + methodName + " " + accessFlags);
        readAttributes();
    }

    private void readAttributes() {
        int attributeCount = readShort();
        for (int i = 0; i < attributeCount; i++) {
            int attributeNameIndex = readShort();
            String attributeName = cpString[attributeNameIndex];
            debug("        attribute " + attributeName);
            int attributeLength = readInt();
            int end = pos + attributeLength;
            if ("Code".equals(attributeName)) {
                readCode();
            }
            pos = end;
        }
    }

    void decompile() {
        int maxStack = readShort();
        int maxLocals = readShort();
        debug("stack: " + maxStack + " locals: " + maxLocals);
        int codeLength = readInt();
        startByteCode = pos;
        int end = pos + codeLength;
        while (pos < end) {
            readByteCode();
        }
        debug("");
        pos = startByteCode + codeLength;
        int exceptionTableLength = readShort();
        pos += 2 * exceptionTableLength;
        readAttributes();
    }

    private void readCode() {
        variables.clear();
        stack.clear();
        int maxStack = readShort();
        int maxLocals = readShort();
        debug("stack: " + maxStack + " locals: " + maxLocals);
        int codeLength = readInt();
        startByteCode = pos;
        if (methodName.startsWith(convertMethodName)) {
            result = getResult();
        }
        pos = startByteCode + codeLength;
        int exceptionTableLength = readShort();
        pos += 2 * exceptionTableLength;
        readAttributes();
    }

    private String getResult() {
        while (true) {
            readByteCode();
            if (end) {
                return stack.pop();
            }
            if (condition) {
                String c = stack.pop();
                Stack<String> currentStack = new Stack<String>();
                currentStack.addAll(stack);
                ArrayList<String> currentVariables = new ArrayList<String>();
                currentVariables.addAll(variables);
                int branch = nextPc;
                String a = getResult();
                stack = currentStack;
                variables = currentVariables;
                pos = branch + startByteCode;
                String b = getResult();
                if (a.equals("0") && b.equals("1")) {
                    return c;
                } else if (a.equals("1") && b.equals("0")) {
                    return "NOT(" +c + ")";
                } else if (b.equals("0")) {
                    return "NOT(" + c + ") AND (" + a + ")";
                } else if (a.equals("0")) {
                    return "(" + c + ") AND (" + b + ")";
                } else if (b.equals("1")) {
                    return "(" + c + ") OR (" + a + ")";
                } else if (a.equals("1")) {
                    return "NOT(" + c + ") AND (" + b + ")";
                }
                return "(" + c + ") ? (" + b + ") : (" + a + ")";
            }
            if (nextPc != 0) {
                pos = nextPc + startByteCode;
            }
        }
    }

    private void readByteCode() {
        int startPos = pos - startByteCode;
        int opCode = readByte();
        String op;
        end = false;
        condition = false;
        nextPc = 0;
        switch(opCode) {
        case 0:
            op = "nop";
            break;
        case 1:
            op = "aconst_null";
            stack.push("null");
            break;
        case 2:
            op = "iconst_m1";
            stack.push("-1");
            break;
        case 3:
            op = "iconst_0";
            stack.push("0");
            break;
        case 4:
            op = "iconst_1";
            stack.push("1");
            break;
        case 5:
            op = "iconst_2";
            stack.push("2");
            break;
        case 6:
            op = "iconst_3";
            stack.push("3");
            break;
        case 7:
            op = "iconst_4";
            stack.push("4");
            break;
        case 8:
            op = "iconst_5";
            stack.push("5");
            break;
        case 9:
            op = "lconst_0";
            stack.push("0");
            break;
        case 10:
            op = "lconst_1";
            stack.push("1");
            break;
        case 11:
            op = "fconst_0";
            stack.push("0.0");
            break;
        case 12:
            op = "fconst_1";
            stack.push("1.0");
            break;
        case 13:
            op = "fconst_2";
            stack.push("2.0");
            break;
        case 14:
            op = "dconst_0";
            stack.push("0.0");
            break;
        case 15:
            op = "dconst_1";
            stack.push("1.0");
            break;
        case 16: {
            int x = (byte) readByte();
            op = "bipush " + x;
            stack.push("" + x);
            break;
        }
        case 17: {
            int x = (short) readShort();
            op = "sipush " + x;
            stack.push("" + x);
            break;
        }
        case 18: {
            String s = getConstant(readByte());
            op = "ldc " + s;
            stack.push(s);
            break;
        }
        case 19: {
            String s = getConstant(readShort());
            op = "ldc_w " + s;
            stack.push(s);
            break;
        }
        case 20: {
            String s = getConstant(readShort());
            op = "ldc2_w " + s;
            stack.push(s);
            break;
        }
        case 21: {
            int x = readByte();
            op = "iload " + x;
            stack.push(getVariable(x));
            break;
        }
        case 22: {
            int x = readByte();
            op = "lload " + x;
            stack.push(getVariable(x));
            break;
        }
        case 23: {
            int x = readByte();
            op = "fload " + x;
            stack.push(getVariable(x));
            break;
        }
        case 24: {
            int x = readByte();
            op = "dload " + x;
            stack.push(getVariable(x));
            break;
        }
        case 25: {
            int x = readByte();
            op = "aload " + x;
            stack.push(getVariable(x));
            break;
        }
        case 26:
            op = "iload_0";
            stack.push(getVariable(0));
            break;
        case 27:
            op = "iload_1";
            stack.push(getVariable(1));
            break;
        case 28:
            op = "iload_2";
            stack.push(getVariable(2));
            break;
        case 29:
            op = "iload_3";
            stack.push(getVariable(3));
            break;
        case 30:
            op = "lload_0";
            stack.push(getVariable(0));
            break;
        case 31:
            op = "lload_1";
            stack.push(getVariable(1));
            break;
        case 32:
            op = "lload_2";
            stack.push(getVariable(2));
            break;
        case 33:
            op = "lload_3";
            stack.push(getVariable(3));
            break;
        case 34:
            op = "fload_0";
            stack.push(getVariable(0));
            break;
        case 35:
            op = "fload_1";
            stack.push(getVariable(1));
            break;
        case 36:
            op = "fload_2";
            stack.push(getVariable(2));
            break;
        case 37:
            op = "fload_3";
            stack.push(getVariable(3));
            break;
        case 38:
            op = "dload_0";
            stack.push(getVariable(0));
            break;
        case 39:
            op = "dload_1";
            stack.push(getVariable(1));
            break;
        case 40:
            op = "dload_2";
            stack.push(getVariable(2));
            break;
        case 41:
            op = "dload_3";
            stack.push(getVariable(3));
            break;
        case 42:
            op = "aload_0";
            stack.push(getVariable(0));
            break;
        case 43:
            op = "aload_1";
            stack.push(getVariable(1));
            break;
        case 44:
            op = "aload_2";
            stack.push(getVariable(2));
            break;
        case 45:
            op = "aload_3";
            stack.push(getVariable(3));
            break;
        case 46: {
            String index = stack.pop();
            String ref = stack.pop();
            op = "iaload";
            stack.push(ref + "[" + index + "]");
            break;
        }
        case 47: {
            String index = stack.pop();
            String ref = stack.pop();
            op = "laload";
            stack.push(ref + "[" + index + "]");
            break;
        }
        case 48: {
            String index = stack.pop();
            String ref = stack.pop();
            op = "faload";
            stack.push(ref + "[" + index + "]");
            break;
        }
        case 49: {
            String index = stack.pop();
            String ref = stack.pop();
            op = "daload";
            stack.push(ref + "[" + index + "]");
            break;
        }
        case 50: {
            String index = stack.pop();
            String ref = stack.pop();
            op = "aaload";
            stack.push(ref + "[" + index + "]");
            break;
        }
        case 51: {
            String index = stack.pop();
            String ref = stack.pop();
            op = "baload";
            stack.push(ref + "[" + index + "]");
            break;
        }
        case 52: {
            String index = stack.pop();
            String ref = stack.pop();
            op = "caload";
            stack.push(ref + "[" + index + "]");
            break;
        }
        case 53: {
            String index = stack.pop();
            String ref = stack.pop();
            op = "saload";
            stack.push(ref + "[" + index + "]");
            break;
        }
        case 54: {
            int var = readByte();
            op = "istore " + var;
            setVariable(var, stack.pop());
            break;
        }
        case 55: {
            int var = readByte();
            op = "lstore " + var;
            setVariable(var, stack.pop());
            break;
        }
        case 56: {
            int var = readByte();
            op = "fstore " + var;
            setVariable(var, stack.pop());
            break;
        }
        case 57: {
            int var = readByte();
            op = "dstore " + var;
            setVariable(var, stack.pop());
            break;
        }
        case 58: {
            int var = readByte();
            op = "astore " + var;
            setVariable(var, stack.pop());
            break;
        }
        case 59:
            op = "istore_0";
            setVariable(0, stack.pop());
            break;
        case 60:
            op = "istore_1";
            setVariable(1, stack.pop());
            break;
        case 61:
            op = "istore_2";
            setVariable(2, stack.pop());
            break;
        case 62:
            op = "istore_3";
            setVariable(3, stack.pop());
            break;
        case 63:
            op = "lstore_0";
            setVariable(0, stack.pop());
            break;
        case 64:
            op = "lstore_1";
            setVariable(1, stack.pop());
            break;
        case 65:
            op = "lstore_2";
            setVariable(2, stack.pop());
            break;
        case 66:
            op = "lstore_3";
            setVariable(3, stack.pop());
            break;
        case 67:
            op = "fstore_0";
            setVariable(0, stack.pop());
            break;
        case 68:
            op = "fstore_1";
            setVariable(1, stack.pop());
            break;
        case 69:
            op = "fstore_2";
            setVariable(2, stack.pop());
            break;
        case 70:
            op = "fstore_3";
            setVariable(3, stack.pop());
            break;
        case 71:
            op = "dstore_0";
            setVariable(0, stack.pop());
            break;
        case 72:
            op = "dstore_1";
            setVariable(1, stack.pop());
            break;
        case 73:
            op = "dstore_2";
            setVariable(2, stack.pop());
            break;
        case 74:
            op = "dstore_3";
            setVariable(3, stack.pop());
            break;
        case 75:
            op = "astore_0";
            setVariable(0, stack.pop());
            break;
        case 76:
            op = "astore_1";
            setVariable(1, stack.pop());
            break;
        case 77:
            op = "astore_2";
            setVariable(2, stack.pop());
            break;
        case 78:
            op = "astore_3";
            setVariable(3, stack.pop());
            break;
        case 79: {
            // String value = stack.pop();
            // String index = stack.pop();
            // String ref = stack.pop();
            op = "iastore";
            // TODO side effect - not supported
            break;
        }
        case 80:
            op = "lastore";
            // TODO side effect - not supported
            break;
        case 81:
            op = "fastore";
            // TODO side effect - not supported
            break;
        case 82:
            op = "dastore";
            // TODO side effect - not supported
            break;
        case 83:
            op = "aastore";
            // TODO side effect - not supported
            break;
        case 84:
            op = "bastore";
            // TODO side effect - not supported
            break;
        case 85:
            op = "castore";
            // TODO side effect - not supported
            break;
        case 86:
            op = "sastore";
            // TODO side effect - not supported
            break;
        case 87:
            op = "pop";
            stack.pop();
            break;
        case 88:
            op = "pop2";
            // TODO currently we don't know the stack types
            stack.pop();
            stack.pop();
            break;
        case 89: {
            op = "dup";
            String x = stack.pop();
            stack.push(x);
            stack.push(x);
            break;
        }
        case 90: {
            op = "dup_x1";
            String a = stack.pop();
            String b = stack.pop();
            stack.push(a);
            stack.push(b);
            stack.push(a);
            break;
        }
        case 91: {
            // TODO currently we don't know the stack types
            op = "dup_x2";
            String a = stack.pop();
            String b = stack.pop();
            String c = stack.pop();
            stack.push(a);
            stack.push(c);
            stack.push(b);
            stack.push(a);
            break;
        }
        case 92: {
            // TODO currently we don't know the stack types
            op = "dup2";
            String a = stack.pop();
            String b = stack.pop();
            stack.push(b);
            stack.push(a);
            stack.push(b);
            stack.push(a);
            break;
        }
        case 93: {
            // TODO currently we don't know the stack types
            op = "dup2_x1";
            String a = stack.pop();
            String b = stack.pop();
            String c = stack.pop();
            stack.push(b);
            stack.push(a);
            stack.push(c);
            stack.push(b);
            stack.push(a);
            break;
        }
        case 94: {
            // TODO currently we don't know the stack types
            op = "dup2_x2";
            String a = stack.pop();
            String b = stack.pop();
            String c = stack.pop();
            String d = stack.pop();
            stack.push(b);
            stack.push(a);
            stack.push(d);
            stack.push(c);
            stack.push(b);
            stack.push(a);
            break;
        }
        case 95: {
            op = "swap";
            String a = stack.pop();
            String b = stack.pop();
            stack.push(a);
            stack.push(b);
            break;
        }
        case 96: {
            String b = stack.pop();
            String a = stack.pop();
            op = "iadd";
            stack.push("(" + a + " + " + b + ")");
            break;
        }
        case 97: {
            String b = stack.pop();
            String a = stack.pop();
            op = "ladd";
            stack.push("(" + a + " + " + b + ")");
            break;
        }
        case 98: {
            String b = stack.pop();
            String a = stack.pop();
            op = "fadd";
            stack.push("(" + a + " + " + b + ")");
            break;
        }
        case 99: {
            String b = stack.pop();
            String a = stack.pop();
            op = "dadd";
            stack.push("(" + a + " + " + b + ")");
            break;
        }
        case 100: {
            String b = stack.pop();
            String a = stack.pop();
            op = "isub";
            stack.push("(" + a + " - " + b + ")");
            break;
        }
        case 101: {
            String b = stack.pop();
            String a = stack.pop();
            op = "lsub";
            stack.push("(" + a + " - " + b + ")");
            break;
        }
        case 102: {
            String b = stack.pop();
            String a = stack.pop();
            op = "fsub";
            stack.push("(" + a + " - " + b + ")");
            break;
        }
        case 103: {
            String b = stack.pop();
            String a = stack.pop();
            op = "dsub";
            stack.push("(" + a + " - " + b + ")");
            break;
        }
        case 104: {
            String b = stack.pop();
            String a = stack.pop();
            op = "imul";
            stack.push("(" + a + " * " + b + ")");
            break;
        }
        case 105: {
            String b = stack.pop();
            String a = stack.pop();
            op = "lmul";
            stack.push("(" + a + " * " + b + ")");
            break;
        }
        case 106: {
            String b = stack.pop();
            String a = stack.pop();
            op = "fmul";
            stack.push("(" + a + " * " + b + ")");
            break;
        }
        case 107: {
            String b = stack.pop();
            String a = stack.pop();
            op = "dmul";
            stack.push("(" + a + " * " + b + ")");
            break;
        }
        case 108: {
            String b = stack.pop();
            String a = stack.pop();
            op = "idiv";
            stack.push("(" + a + " / " + b + ")");
            break;
        }
        case 109: {
            String b = stack.pop();
            String a = stack.pop();
            op = "ldiv";
            stack.push("(" + a + " / " + b + ")");
            break;
        }
        case 110: {
            String b = stack.pop();
            String a = stack.pop();
            op = "fdiv";
            stack.push("(" + a + " / " + b + ")");
            break;
        }
        case 111: {
            String b = stack.pop();
            String a = stack.pop();
            op = "ddiv";
            stack.push("(" + a + " / " + b + ")");
            break;
        }
        case 112: {
            String b = stack.pop();
            String a = stack.pop();
            op = "irem";
            stack.push("(" + a + " % " + b + ")");
            break;
        }
        case 113: {
            String b = stack.pop();
            String a = stack.pop();
            op = "lrem";
            stack.push("(" + a + " % " + b + ")");
            break;
        }
        case 114: {
            String b = stack.pop();
            String a = stack.pop();
            op = "frem";
            stack.push("(" + a + " % " + b + ")");
            break;
        }
        case 115: {
            String b = stack.pop();
            String a = stack.pop();
            op = "drem";
            stack.push("(" + a + " % " + b + ")");
            break;
        }
        case 116:
            op = "ineg";
            break;
        case 117:
            op = "lneg";
            break;
        case 118:
            op = "fneg";
            break;
        case 119:
            op = "dneg";
            break;
        case 120:
            op = "ishl";
            break;
        case 121:
            op = "lshl";
            break;
        case 122:
            op = "ishr";
            break;
        case 123:
            op = "lshr";
            break;
        case 124:
            op = "iushr";
            break;
        case 125:
            op = "lushr";
            break;
        case 126:
            op = "iand";
            break;
        case 127:
            op = "land";
            break;
        case 128:
            op = "ior";
            break;
        case 129:
            op = "lor";
            break;
        case 130:
            op = "ixor";
            break;
        case 131:
            op = "lxor";
            break;
        case 132: {
            int var = readByte();
            int off = (byte) readByte();
            op = "iinc " + var + " " + off;
            break;
        }
        case 133:
            op = "i2l";
            break;
        case 134:
            op = "i2f";
            break;
        case 135:
            op = "i2d";
            break;
        case 136:
            op = "l2i";
            break;
        case 137:
            op = "l2f";
            break;
        case 138:
            op = "l2d";
            break;
        case 139:
            op = "f2i";
            break;
        case 140:
            op = "f2l";
            break;
        case 141:
            op = "f2d";
            break;
        case 142:
            op = "d2i";
            break;
        case 143:
            op = "d2l";
            break;
        case 144:
            op = "d2f";
            break;
        case 145:
            op = "i2b";
            break;
        case 146:
            op = "i2c";
            break;
        case 147:
            op = "i2s";
            break;
        case 148: {
            String b = stack.pop(), a = stack.pop();
            stack.push("SIGN(" + a + " - " + b + ")");
            op = "lcmp";
            break;
        }
        case 149:
            op = "fcmpl";
            break;
        case 150:
            op = "fcmpg";
            break;
        case 151:
            op = "dcmpl";
            break;
        case 152:
            op = "dcmpg";
            break;
        case 153:
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            stack.push("(" + stack.pop() + " = 0)");
            op = "ifeq " + nextPc;
            break;
        case 154:
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            stack.push("(" + stack.pop() + " <> 0)");
            op = "ifne " + nextPc;
            break;
        case 155:
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            stack.push("(" + stack.pop() + " < 0)");
            op = "iflt " + nextPc;
            break;
        case 156:
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            stack.push("(" + stack.pop() + " >= 0)");
            op = "ifge " + nextPc;
            break;
        case 157:
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            stack.push("(" + stack.pop() + " > 0)");
            op = "ifgt " + nextPc;
            break;
        case 158:
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            stack.push("(" + stack.pop() + "<= 0)");
            op = "ifle " + nextPc;
            break;
        case 159: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            String b = stack.pop(), a = stack.pop();
            stack.push("(" + a + " = " + b + ")");
            op = "if_icmpeq " + nextPc;
            break;
        }
        case 160: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            String b = stack.pop(), a = stack.pop();
            stack.push("(" + a + " <> " + b + ")");
            op = "if_icmpne " + nextPc;
            break;
        }
        case 161: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            String b = stack.pop(), a = stack.pop();
            stack.push("(" + a + " < " + b + ")");
            op = "if_icmplt " + nextPc;
            break;
        }
        case 162: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            String b = stack.pop(), a = stack.pop();
            stack.push("(" + a + " >= " + b + ")");
            op = "if_icmpge " + nextPc;
            break;
        }
        case 163: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            String b = stack.pop(), a = stack.pop();
            stack.push("(" + a + " > " + b + ")");
            op = "if_icmpgt " + nextPc;
            break;
        }
        case 164: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            String b = stack.pop(), a = stack.pop();
            stack.push("(" + a + " <= " + b + ")");
            op = "if_icmple " + nextPc;
            break;
        }
        case 165: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            String b = stack.pop(), a = stack.pop();
            stack.push("(" + a + " = " + b + ")");
            op = "if_acmpeq " + nextPc;
            break;
        }
        case 166: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            String b = stack.pop(), a = stack.pop();
            stack.push("(" + a + " <> " + b + ")");
            op = "if_acmpne " + nextPc;
            break;
        }
        case 167:
            nextPc = getAbsolutePos(pos, readShort());
            op = "goto " + nextPc;
            break;
        case 168:
            // TODO not supported yet
            op = "jsr " + getAbsolutePos(pos, readShort());
            break;
        case 169:
            // TODO not supported yet
            op = "ret " + readByte();
            break;
        case 170: {
            int start = pos;
            pos += 4 - ((pos - startByteCode) & 3);
            int def = readInt();
            int low = readInt(), high = readInt();
            int n = high - low + 1;
            op = "tableswitch default:" + getAbsolutePos(start, def);
            StringBuilder buff = new StringBuilder();
            for (int i = 0; i < n; i++) {
                buff.append(' ').append(low++).append(":").append(getAbsolutePos(start, readInt()));
            }
            op += buff.toString();
            // pos += n * 4;
            break;
        }
        case 171: {
            int start = pos;
            pos += 4 - ((pos - startByteCode) & 3);
            int def = readInt();
            int n = readInt();
            op = "lookupswitch default:" + getAbsolutePos(start, def);
            StringBuilder buff = new StringBuilder();
            for (int i = 0; i < n; i++) {
                buff.append(' ').append(readInt()).append(":").append(getAbsolutePos(start, readInt()));
            }
            op += buff.toString();
            // pos += n * 8;
            break;
        }
        case 172:
            op = "ireturn";
            end = true;
            break;
        case 173:
            op = "lreturn";
            end = true;
            break;
        case 174:
            op = "freturn";
            end = true;
            break;
        case 175:
            op = "dreturn";
            end = true;
            break;
        case 176:
            op = "areturn";
            end = true;
            break;
        case 177:
            op = "return";
            // no value returned
            stack.push(null);
            end = true;
            break;
        case 178:
            op = "getstatic " + getField(readShort());
            break;
        case 179:
            op = "putstatic " + getField(readShort());
            break;
        case 180: {
            String field = getField(readShort());
            String p = stack.pop();
            p = p + "." + field.substring(Math.max(field.lastIndexOf('$'), field.lastIndexOf('.')) + 1, field.indexOf(' '));
            if (p.startsWith("this.")) {
                p = p.substring(5);
            }
            stack.push(p);
            op = "getfield " + field;
            break;
        }
        case 181:
            op = "putfield " + getField(readShort());
            break;
        case 182: {
            String method = getMethod(readShort());
            op = "invokevirtual " + method;
            if (method.equals("java/lang/String.equals (Ljava/lang/Object;)Z")) {
                String a = stack.pop();
                String b = stack.pop();
                stack.push("(" + a + " = " + b + ")");
            } else if (method.equals("java/lang/Integer.intValue ()I")) {
                // ignore
            } else if (method.equals("java/lang/Long.longValue ()J")) {
                // ignore
            }
            break;
        }
        case 183:
            op = "invokespecial " + getMethod(readShort());
            break;
        case 184:
            op = "invokestatic " + getMethod(readShort());
            break;
        case 185: {
            int methodRef = readShort();
            readByte();
            readByte();
            op = "invokeinterface " + getMethod(methodRef);
            break;
        }
        case 187:
            op = "new " + cpString[cpInt[readShort()]];
            break;
        case 188:
            op = "newarray " + readByte();
            break;
        case 189:
            op = "anewarray " + cpString[readShort()];
            break;
        case 190:
            op = "arraylength";
            break;
        case 191:
            op = "athrow";
            break;
        case 192:
            op = "checkcast " + cpString[readShort()];
            break;
        case 193:
            op = "instanceof " + cpString[readShort()];
            break;
        case 194:
            op = "monitorenter";
            break;
        case 195:
            op = "monitorexit";
            break;
        case 196: {
            opCode = readByte();
            switch (opCode) {
            case 21:
                op = "wide iload " + readShort();
                break;
            case 22:
                op = "wide lload " + readShort();
                break;
            case 23:
                op = "wide fload " + readShort();
                break;
            case 24:
                op = "wide dload " + readShort();
                break;
            case 25:
                op = "wide aload " + readShort();
                break;
            case 54:
                op = "wide istore " + readShort();
                break;
            case 55:
                op = "wide lstore " + readShort();
                break;
            case 56:
                op = "wide fstore " + readShort();
                break;
            case 57:
                op = "wide dstore " + readShort();
                break;
            case 58:
                op = "wide astore " + readShort();
                break;
            case 132: {
                int var = readShort();
                int off = (short) readShort();
                op = "wide iinc " + var + " " + off;
                break;
            }
            case 169:
                op = "wide ret " + readShort();
                break;
            default:
                throw new Error("unsupported wide opCode " + opCode);
            }
            break;
        }
        case 197:
            op = "multianewarray " + cpString[readShort()] + " " + readByte();
            break;
        case 198: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            String a = stack.pop();
            stack.push("(" + a + " IS NULL)");
            op = "ifnull " + nextPc;
            break;
        }
        case 199: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            String a = stack.pop();
            stack.push("(" + a + " IS NOT NULL)");
            op = "ifnonnull " + nextPc;
            break;
        }
        case 200:
            op = "goto_w " + getAbsolutePos(pos, readInt());
            break;
        case 201:
            op = "jsr_w " + getAbsolutePos(pos, readInt());
            break;
        default:
            throw new Error("unsupported opCode " + opCode);
        }
        debug("    " + startPos + ": " + op);
    }

    private void setVariable(int x, String value) {
        while (x >= variables.size()) {
            variables.add("p" + variables.size());
        }
        variables.set(x, value);
    }

    private String getVariable(int x) {
        if (x == 0) {
            return "this";
        }
        while (x >= variables.size()) {
            variables.add("p" + variables.size());
        }
        return variables.get(x);
    }

    private String getField(int fieldRef) {
        int field = cpInt[fieldRef];
        int classIndex = field >>> 16;
        int nameAndType = cpInt[field & 0xffff];
        String className = cpString[cpInt[classIndex]] + "." + cpString[nameAndType >>> 16] + " " + cpString[nameAndType & 0xffff];
        return className;
    }

    private String getMethod(int methodRef) {
        int method = cpInt[methodRef];
        int classIndex = method >>> 16;
        int nameAndType = cpInt[method & 0xffff];
        String className = cpString[cpInt[classIndex]] + "." + cpString[nameAndType >>> 16] + " " + cpString[nameAndType & 0xffff];
        return className;
    }

    private String getConstant(int constantRef) {
        switch (cpType[constantRef]) {
        case 3:
            // int
            return cpString[constantRef];
        case 4:
            // float
            return cpString[constantRef];
        case 5:
            // long
            return cpString[constantRef];
        case 6:
            // double
            return cpString[constantRef];
        case 8:
            // string
            // TODO escape
            return "\"" + cpString[cpInt[constantRef]] + "\"";
        default:
            throw new Error("not a constant: " + constantRef);
        }
    }

    private String readString() {
        int size = readShort();
        byte[] buff = data;
        int p = pos, end = p + size;
        char[] chars = new char[size];
        int j = 0;
        for (; p < end; j++) {
            int x = buff[p++] & 0xff;
            if (x < 0x80) {
                chars[j] = (char) x;
            } else if (x >= 0xe0) {
                chars[j] = (char) (((x & 0xf) << 12) + ((buff[p++] & 0x3f) << 6) + (buff[p++] & 0x3f));
            } else {
                chars[j] = (char) (((x & 0x1f) << 6) + (buff[p++] & 0x3f));
            }
        }
        pos = p;
        return new String(chars, 0, j);
    }

    private int getAbsolutePos(int pos, int offset) {
        return pos - startByteCode - 1 + (short) offset;
    }

    private int readByte() {
        return data[pos++] & 0xff;
    }

    private int readShort() {
        byte[] buff = data;
        return ((buff[pos++] & 0xff) << 8) + (buff[pos++] & 0xff);
    }

    private int readInt() {
        byte[] buff = data;
        return (buff[pos++] << 24) + ((buff[pos++] & 0xff) << 16) + ((buff[pos++] & 0xff) << 8) + (buff[pos++] & 0xff);
    }

    private long readLong() {
        return ((long) (readInt()) << 32) + (readInt() & 0xffffffffL);
    }

}
