/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.h2.test.trace;

import java.util.ArrayList;

/**
 * The parser to parse a statement (a single line in the log file).
 *
 * @author Thomas Mueller
 *
 */
class Parser {
    private static final int STRING = 0, NAME = 1, NUMBER = 2, SPECIAL = 3;

    private Player player;

    private Statement stat;

    private String line;

    private String token;

    private int tokenType;

    private int pos;

    static Statement parseStatement(Player player, String line) {
        Parser p = new Parser(player, line);
        p.parseStatement();
        return p.stat;
    }

    private Parser(Player player, String line) {
        this.player = player;
        this.line = line;
        read();
    }

    private Statement parseStatement() {
        stat = new Statement(player);
        String name = readToken();
        Object o = player.getObject(name);
        if (o != null) {
            // n0.getPath()
            read(".");
            String methodName = readToken();
            parseCall(name, o, methodName);
        } else {
            if (readIf(".")) {
                // java.lang.System.exit(0);
                parseStaticCall(name);
            } else {
                // Node n0 = ...
                if (readIf("[")) {
                    read("]");
                }
                stat.setAssign(name, readToken());
                read("=");
                name = readToken();
                o = player.getObject(name);
                if (o != null) {
                    // ... = s0.getRootNode();
                    read(".");
                    parseCall(name, o, readToken());
                } else if (readIf(".")) {
                    // ... = x.y.open("...");
                    parseStaticCall(name);
                }
            }
        }
        return stat;
    }

    private void read() {
        while (line.charAt(pos) == ' ') {
            pos++;
        }
        int start = pos;
        char ch = line.charAt(pos);
        switch (ch) {
        case '\"':
            tokenType = STRING;
            pos++;
            while (pos < line.length()) {
                ch = line.charAt(pos);
                if (ch == '\\') {
                    pos += 2;
                } else if (ch == '\"') {
                    pos++;
                    break;
                } else {
                    pos++;
                }
            }
            break;
        case '.':
        case ',':
        case '(':
        case ')':
        case ';':
        case '{':
        case '}':
        case '[':
        case ']':
        case '=':
            tokenType = SPECIAL;
            pos++;
            break;
        default:
            if (Character.isLetter(ch) || ch == '_') {
                tokenType = NAME;
                pos++;
                while (true) {
                    ch = line.charAt(pos);
                    if (Character.isLetterOrDigit(ch) || ch == '_') {
                        pos++;
                    } else {
                        break;
                    }
                }
            } else if (ch == '-' || Character.isDigit(ch)) {
                tokenType = NUMBER;
                pos++;
                while (true) {
                    ch = line.charAt(pos);
                    if (Character.isDigit(ch)
                            || ".+-eElLxabcdefABCDEF".indexOf(ch) >= 0) {
                        pos++;
                    } else {
                        break;
                    }
                }
            }
        }
        token = line.substring(start, pos);
    }

    private boolean readIf(String s) {
        if (token.equals(s)) {
            read();
            return true;
        }
        return false;
    }

    private String readToken() {
        String s = token;
        read();
        return s;
    }

    private void read(String s) {
        if (!readIf(s)) {
            throw new Error("expected: " + s + " read: " + token + " in "
                    + line);
        }
    }

    private Arg parseValue() {
        if (tokenType == STRING) {
            String s = readToken();
            s = StringTools.javaDecode(s.substring(1, s.length() - 1));
            return new Arg(player, String.class, s);
        } else if (tokenType == NUMBER) {
            String number = readToken().toLowerCase();
            if (number.startsWith("0x")) {
                if (number.endsWith("l")) {
                    Long v = new Long(Long.parseLong(number.substring(2, number
                            .length() - 1), 16));
                    return new Arg(player, long.class, v);
                } else {
                    Integer v = new Integer(Integer.parseInt(number.substring(
                            2, number.length() - 1), 16));
                    return new Arg(player, int.class, v);
                }
            } else if (number.indexOf("e") >= 0 || number.indexOf(".") >= 0) {
                Double v = new Double(Double.parseDouble(number));
                return new Arg(player, double.class, v);
            } else if (number.endsWith("l")) {
                Long v = new Long(Long.parseLong(number.substring(0, number
                        .length() - 1)));
                return new Arg(player, long.class, v);
            } else {
                Integer v = new Integer(Integer.parseInt(number));
                return new Arg(player, int.class, v);
            }
        } else if (tokenType == NAME) {
            if (readIf("true")) {
                return new Arg(player, boolean.class, Boolean.TRUE);
            } else if (readIf("false")) {
                return new Arg(player, boolean.class, Boolean.FALSE);
            } else if (readIf("null")) {
                throw new Error(
                        "Null: class not specified. Example: (java.lang.String)null");
            } else if (readIf("new")) {
                if (readIf("String")) {
                    read("[");
                    read("]");
                    read("{");
                    ArrayList values = new ArrayList();
                    do {
                        values.add(parseValue().getValue());
                    } while (readIf(","));
                    read("}");
                    String[] list = new String[values.size()];
                    values.toArray(list);
                    return new Arg(player, String[].class, list);
                } else {
                    throw new Error("Unsupported constructor: " + readToken());
                }
            }
            String name = readToken();
            Object obj = player.getObject(name);
            if (obj != null) {
                return new Arg(player, obj.getClass(), obj);
            }
            read(".");
            Statement outer = stat;
            stat = new Statement(player);
            parseStaticCall(name);
            Arg s = new Arg(stat);
            stat = outer;
            return s;
        } else if (readIf("(")) {
            String className = readToken();
            className = parseClassName(className);
            if (readIf("[")) {
                read("]");
            }
            className = "[L" + className + ";";
            read(")");
            read("null");
            Class c = Player.getClass(className);
            return new Arg(player, c, null);
        } else {
            throw new Error("Expected value, got: " + readToken() + " in "
                    + line);
        }
    }

    private String parseClassName(String clazz) {
        while (readIf(".")) {
            clazz += "." + readToken();
        }
        return clazz;
    }

    private void parseCall(String objectName, Object o, String methodName) {
        stat.setMethodCall(objectName, o, methodName);
        ArrayList args = new ArrayList();
        read("(");
        while (true) {
            if (readIf(")")) {
                break;
            }
            Arg p = parseValue();
            args.add(p);
            if (readIf(")")) {
                break;
            }
            read(",");
        }
        stat.setArgs(args);
    }

    private void parseStaticCall(String clazz) {
        String last = readToken();
        // clazz += "." + readToken();
        while (readIf(".")) {
            clazz += last == null ? "" : "." + last;
            last = readToken();
        }
        String methodName = last;
        stat.setStaticCall(clazz);
        parseCall(null, null, methodName);
    }

}
