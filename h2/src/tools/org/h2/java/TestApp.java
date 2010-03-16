/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java;


/**
 * A test application.
 */
public class TestApp {

/* c:

int main(int argc, char** argv) {
    org_h2_java_TestApp_main(null);
}

*/

    /**
     * Run this application.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) {
        System.out.println("Hello World");
        System.out.println("Hello!");
    }

    /**
     * A test method.
     *
     * @param name ignored
     * @param x ignored
     * @return ignored
     */
    public int getName(int name, int x) {
        System.out.println("Hello");
        int m = x;
        // m = FINAL_VALUE;
        switch (x) {
        case 1:
            m = 3;
            m = 4;
            break;
        default:
            m = 4;
            m = 5;
        }
        for (int i = 0; i < 10; i++, i--) {
            getName(0, 0);
        }
        if (m > 0) {
            getName(2, 3);
        } else {
            getName(1, 12);
        }
        do {
            getName(0, 0);
            return name;
        } while (true);
    }

}
