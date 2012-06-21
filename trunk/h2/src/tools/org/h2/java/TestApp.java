/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
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
    org_h2_java_TestApp_main(ptr<array<ptr<java_lang_String> > >());
}

*/

    /**
     * Run this application.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) {
        for (int i = 0; i < 10; i++) {
            System.out.println("Hello " + i);
        }
    }

}
