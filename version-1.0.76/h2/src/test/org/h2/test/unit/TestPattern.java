/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import org.h2.expression.CompareLike;
import org.h2.test.TestBase;
import org.h2.value.CompareMode;

/**
 * Tests LIKE pattern matching.
 */
public class TestPattern extends TestBase {

    public void test() throws Exception {
        CompareMode mode = new CompareMode(null, null, 100);
        CompareLike comp = new CompareLike(mode, null, null, null, false);
        test(comp, "B", "%_");
        test(comp, "A", "A%");
        test(comp, "A", "A%%");
        test(comp, "A_A", "%\\_%");

        for (int i = 0; i < 10000; i++) {
            String pattern = getRandomPattern();
            String value = getRandomValue();
            test(comp, value, pattern);
        }
    }

    private void test(CompareLike comp, String value, String pattern) throws Exception {
        String regexp = initPatternRegexp(pattern, '\\');
        boolean resultRegexp = value.matches(regexp);
        boolean result = comp.test(pattern, value, '\\');
        if (result != resultRegexp) {
            fail("Error: >" + value + "< LIKE >" + pattern + "< result=" + result + " resultReg=" + resultRegexp);
        }
    }

    private static String getRandomValue() {
        StringBuffer buff = new StringBuffer();
        int len = (int) (Math.random() * 10);
        String s = "AB_%\\";
        for (int i = 0; i < len; i++) {
            buff.append(s.charAt((int) (Math.random() * s.length())));
        }
        return buff.toString();
    }

    private static String getRandomPattern() {
        StringBuffer buff = new StringBuffer();
        int len = (int) (Math.random() * 4);
        String s = "A%_\\";
        for (int i = 0; i < len; i++) {
            char c = s.charAt((int) (Math.random() * s.length()));
            if ((c == '_' || c == '%') && Math.random() > 0.5) {
                buff.append('\\');
            } else if (c == '\\') {
                buff.append(c);
            }
            buff.append(c);
        }
        return buff.toString();
    }

    private String initPatternRegexp(String pattern, char escape) throws Exception {
        int len = pattern.length();
        StringBuffer buff = new StringBuffer();
        for (int i = 0; i < len; i++) {
            char c = pattern.charAt(i);
            if (escape == c) {
                if (i >= len) {
                    fail("escape can't be last char");
                }
                c = pattern.charAt(++i);
                buff.append('\\');
                buff.append(c);
            } else if (c == '%') {
                buff.append(".*");
            } else if (c == '_') {
                buff.append('.');
            } else if (c == '\\') {
                buff.append("\\\\");
            } else {
                buff.append(c);
            }
            // TODO regexp: there are other chars that need escaping
        }
        String regexp = buff.toString();
        // System.out.println("regexp = " + regexp);
        return regexp;
    }

}
