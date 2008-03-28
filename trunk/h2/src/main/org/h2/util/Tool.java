/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.PrintStream;
import java.sql.SQLException;

/**
 * Command line tools implement the tool interface so that they can be used in
 * the H2 Console.
 */
public abstract class Tool {
    
    protected PrintStream out = System.out;
    protected PrintStream err = System.err;
    
    /**
     * Sets the standard output stream.
     * 
     * @param out the new standard output stream
     */
    public void setOut(PrintStream out) {
        this.out = out;
    }

    /**
     * Run the tool with the given output stream and arguments.
     * 
     * @param out the print stream, for example System.out
     * @param args the argument list
     */
    public abstract void run(String[] args) throws SQLException;
    
}
