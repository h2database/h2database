/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.StringReader;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.util.ScriptReader;

public class TestScriptReader extends TestBase {

    public void test() throws Exception {
        testCommon();
        testRandom();
    }

    private void testRandom() throws Exception {
        int len = getSize(1000, 10000);
        Random random = new Random(10);
        for (int i = 0; i < len; i++) {
            int l = random.nextInt(10);
            String[] sql = new String[l];
            StringBuffer buff = new StringBuffer();
            for (int j = 0; j < l; j++) {
                sql[j] = randomStatement(random);
                buff.append(sql[j]);
                if (j < l - 1) {
                    buff.append(";");
                }
            }
            String s = buff.toString();
            StringReader reader = new StringReader(s);
            ScriptReader source = new ScriptReader(reader);
            for (int j = 0; j < l; j++) {
                String e = source.readStatement();
                String c = sql[j];
                if (c.length() == 0 && j == l - 1) {
                    c = null;
                }
                check(e, c);
            }
            check(source.readStatement(), null);
        }
    }

    private String randomStatement(Random random) {
        StringBuffer buff = new StringBuffer();
        int len = random.nextInt(5);
        for (int i = 0; i < len; i++) {
            switch (random.nextInt(10)) {
            case 0: {
                int l = random.nextInt(4);
                String[] ch = new String[] { "\n", "\r", " ", "*", "a", "0" };
                for (int j = 0; j < l; j++) {
                    buff.append(ch[random.nextInt(ch.length)]);
                }
                break;
            }
            case 1: {
                buff.append('\'');
                int l = random.nextInt(4);
                String[] ch = new String[] { ";", "\n", "\r", "--", "//", "/", "-", "*", "/*", "*/", "\"" };
                for (int j = 0; j < l; j++) {
                    buff.append(ch[random.nextInt(ch.length)]);
                }
                buff.append('\'');
                break;
            }
            case 2: {
                buff.append('"');
                int l = random.nextInt(4);
                String[] ch = new String[] { ";", "\n", "\r", "--", "//", "/", "-", "*", "/*", "*/", "\'" };
                for (int j = 0; j < l; j++) {
                    buff.append(ch[random.nextInt(ch.length)]);
                }
                buff.append('"');
                break;
            }
            case 3: {
                buff.append('-');
                if (random.nextBoolean()) {
                    String[] ch = new String[] { "\n", "\r", "*", "a", " " };
                    int l = 1 + random.nextInt(4);
                    for (int j = 0; j < l; j++) {
                        buff.append(ch[random.nextInt(ch.length)]);
                    }
                } else {
                    buff.append('-');
                    String[] ch = new String[] { ";", "-", "//", "/*", "*/", "a" };
                    int l = random.nextInt(4);
                    for (int j = 0; j < l; j++) {
                        buff.append(ch[random.nextInt(ch.length)]);
                    }
                    buff.append('\n');
                }
                break;
            }
            case 4: {
                buff.append('/');
                if (random.nextBoolean()) {
                    String[] ch = new String[] { "\n", "\r", "a", " ", "- " };
                    int l = 1 + random.nextInt(4);
                    for (int j = 0; j < l; j++) {
                        buff.append(ch[random.nextInt(ch.length)]);
                    }
                } else {
                    buff.append('*');
                    String[] ch = new String[] { ";", "-", "//", "/* ", "--", "\n", "\r", "a" };
                    int l = random.nextInt(4);
                    for (int j = 0; j < l; j++) {
                        buff.append(ch[random.nextInt(ch.length)]);
                    }
                    buff.append("*/");
                }
                break;
            }
            }
        }
        return buff.toString();
    }

    private void testCommon() throws Exception {
        String s = "a;';';\";\";--;\n;/*;\n*/;//;\na;";
        StringReader reader = new StringReader(s);
        ScriptReader source = new ScriptReader(reader);
        check(source.readStatement(), "a");
        check(source.readStatement(), "';'");
        check(source.readStatement(), "\";\"");
        check(source.readStatement(), "--;\n");
        check(source.readStatement(), "/*;\n*/");
        check(source.readStatement(), "//;\na");
        check(source.readStatement(), null);
        source.close();
    }

}
