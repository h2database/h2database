/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java;

import java.util.ArrayList;

/**
 * A statement.
 */
public interface Statement {

    boolean isEnd();

    // toString

}

/**
 * The base class for statements.
 */
abstract class StatementBase implements Statement {

    public boolean isEnd() {
        return false;
    }

}

/**
 * A "return" statement.
 */
class ReturnStatement extends StatementBase {

    Expr expr;

    public String toString() {
        if (expr == null) {
            return "return;";
        }
        if (expr.getType().isSimplePrimitive()) {
            return "return " + expr + ";";
        }
        return "return " + JavaParser.toCType(expr.getType()) + "(" + expr + ");";
    }

}

/**
 * A "do .. while" statement.
 */
class DoWhileStatement extends StatementBase {

    Expr condition;
    Statement block;

    public String toString() {
        return "do {\n" + block + "} while (" + condition + ");";
    }

}

/**
 * A "continue" statement.
 */
class ContinueStatement extends StatementBase {

    public String toString() {
        return "continue;";
    }

}

/**
 * A "break" statement.
 */
class BreakStatement extends StatementBase {

    public String toString() {
        return "break;";
    }

}

/**
 * An empty statement.
 */
class EmptyStatement extends StatementBase {

    public String toString() {
        return ";";
    }

}

/**
 * A "switch" statement.
 */
class SwitchStatement extends StatementBase {

    Expr expr;
    StatementBlock defaultBlock;
    ArrayList<Expr> cases = new ArrayList<Expr>();
    ArrayList<StatementBlock> blocks = new  ArrayList<StatementBlock>();

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("switch (").append(expr).append(") {\n");
        for (int i = 0; i < cases.size(); i++) {
            buff.append("case " + cases.get(i) + ":\n");
            buff.append(blocks.get(i).toString());
        }
        if (defaultBlock != null) {
            buff.append("default:\n");
            buff.append(defaultBlock.toString());
        }
        buff.append("}");
        return buff.toString();
    }

}

/**
 * An expression statement.
 */
class ExprStatement extends StatementBase {

    Expr expr;

    public String toString() {
        return expr + ";";
    }

}

/**
 * A "while" statement.
 */
class WhileStatement extends StatementBase {

    Expr condition;
    Statement block;

    public String toString() {
        String w = "while (" + condition + ")";
        String s = block.toString();
        return w + "\n" + s;
    }

}

/**
 * An "if" statement.
 */
class IfStatement extends StatementBase {

    Expr condition;
    Statement block;
    Statement elseBlock;

    public String toString() {
        String w = "if (" + condition + ") {\n";
        String s = block.toString();
        if (elseBlock != null) {
            s += "} else {\n" + elseBlock.toString();
        }
        return w + s + "}";
    }

}

/**
 * A "for" statement.
 */
class ForStatement extends StatementBase {

    Statement init;
    Expr condition;
    Expr update;
    Statement block;
    ArrayList<Expr> updates = new ArrayList<Expr>();
    Type iterableType;
    String iterableVariable;
    Expr iterable;

    public String toString() {
        StringBuffer buff = new StringBuffer();
        buff.append("for (");
        if (iterableType != null) {
            Type it = iterable.getType();
            if (it != null && it.arrayLevel > 0) {
                String idx = "i_" + iterableVariable;
                buff.append("int " + idx + " = 0; " + idx + " < " + iterable + "->length(); " + idx + "++");
                buff.append(") {\n");
                buff.append(JavaParser.indent(iterableType + " " + iterableVariable + " = " + iterable + "->at("+ idx +");\n"));
                buff.append(block.toString()).append("}");
            } else {
                // TODO iterate over a collection
                buff.append(iterableType).append(' ');
                buff.append(iterableVariable).append(": ");
                buff.append(iterable);
                buff.append(") {\n");
                buff.append(block.toString()).append("}");
            }
        } else {
            buff.append(init.toString());
            buff.append(" ").append(condition.toString()).append("; ");
            for (int i = 0; i < updates.size(); i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append(updates.get(i));
            }
            buff.append(") {\n");
            buff.append(block.toString()).append("}");
        }
        return buff.toString();
    }

}

/**
 * A statement block.
 */
class StatementBlock extends StatementBase {

    ArrayList<Statement> instructions = new ArrayList<Statement>();

    public String toString() {
        StringBuilder buff = new StringBuilder();
        for (Statement s : instructions) {
            if (s.isEnd()) {
                break;
            }
            buff.append(JavaParser.indent(s.toString()));
        }
        return buff.toString();
    }

}

/**
 * A variable declaration.
 */
class VarDecStatement extends StatementBase {

    Type type;
    ArrayList<String> variables = new ArrayList<String>();
    ArrayList<Expr> values = new ArrayList<Expr>();

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append(JavaParser.toCType(type)).append(' ');
        StringBuilder assign = new StringBuilder();
        for (int i = 0; i < variables.size(); i++) {
            if (i > 0) {
                buff.append(", ");
            }
            String varName = variables.get(i);
            buff.append(varName);
            Expr value = values.get(i);
            if (value != null) {
                if (value.getType().isSimplePrimitive()) {
                    buff.append(" = ").append(value);
                } else {
                    assign.append(varName).append(" = ").append(value).append(";\n");
                }
            }
        }
        buff.append(";");
        if (assign.length() > 0) {
            buff.append("\n");
            buff.append(assign);
        }
        return buff.toString();
    }

}

/**
 * A native statement.
 */
class StatementNative extends StatementBase {

    String code;

    public String toString() {
        return code;
    }

    public boolean isEnd() {
        return code.equals("return;");
    }

}

