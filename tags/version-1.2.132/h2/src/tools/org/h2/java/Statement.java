/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
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

    // toString

}

/**
 * A "return" statement.
 */
class ReturnStatement implements Statement {
    Expr expr;
    public String toString() {
        return "return " + (expr == null ? "" : expr) + ";";
    }
}

/**
 * A "do .. while" statement.
 */
class DoWhileStatement implements Statement {
    Expr condition;
    Statement block;
    public String toString() {
        return "do {\n" + block + "} while (" + condition + ");";
    }
}

/**
 * A "continue" statement.
 */
class ContinueStatement implements Statement {
    public String toString() {
        return "continue;";
    }
}

/**
 * A "break" statement.
 */
class BreakStatement implements Statement {
    public String toString() {
        return "break;";
    }
}

/**
 * An empty statement.
 */
class EmptyStatement implements Statement {
    public String toString() {
        return ";";
    }
}

/**
 * A "switch" statement.
 */
class SwitchStatement implements Statement {
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
class ExprStatement implements Statement {
    Expr expr;
    public String toString() {
        return expr + ";";
    }
}

/**
 * A "while" statement.
 */
class WhileStatement implements Statement {
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
class IfStatement implements Statement {
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
class ForStatement implements Statement {
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
                buff.append("int " + idx + " = 0; " + idx + " < LENGTH(" + iterable + "); " + idx + "++");
                buff.append(") {\n");
                buff.append(JavaParser.indent(iterableType + " " + iterableVariable + " = " + iterable + "["+ idx +"];\n"));
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
class StatementBlock implements Statement {
    ArrayList<Statement> instructions = new ArrayList<Statement>();
    public String toString() {
        StringBuilder buff = new StringBuilder();
        for (Statement s : instructions) {
            buff.append(JavaParser.indent(s.toString()));
        }
        return buff.toString();
    }
}

/**
 * A variable declaration.
 */
class VarDecStatement implements Statement {
    Type type;
    ArrayList<String> variables = new ArrayList<String>();
    ArrayList<Expr> values = new ArrayList<Expr>();
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append(type).append(' ');
        for (int i = 0; i < variables.size(); i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(variables.get(i));
            Expr value = values.get(i);
            if (value != null) {
                buff.append(" = ").append(value);
            }
        }
        buff.append(";");
        return buff.toString();
    }
}

/**
 * A native statement.
 */
class StatementNative implements Statement {
    String code;
    public String toString() {
        return code;
    }
}

