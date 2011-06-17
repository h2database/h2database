/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
//import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;

import org.h2.message.Message;
import org.h2.message.TraceSystem;

/**
 * Sorted properties file.
 * This implementation requires that store() internally calls keys().
 */
public class SortedProperties extends Properties {

    private static final long serialVersionUID = 5657650728102821923L;

    public synchronized Enumeration<Object> keys() {
        Vector<Object> v = new Vector<Object>(keySet());
        Collections.sort(v, new Comparator<Object>() {
            public int compare(Object o1, Object o2) {
                return o1.toString().compareTo(o2.toString());
            }
        });
        return v.elements();
    }

    /**
     * Get a boolean property value from a properties object.
     *
     * @param prop the properties object
     * @param key the key
     * @param def the default value
     * @return the value if set, or the default value if not
     */
    public static boolean getBooleanProperty(Properties prop, String key, boolean def) {
        String value = prop.getProperty(key, ""+def);
        try {
            return Boolean.valueOf(value).booleanValue();
        } catch (Exception e) {
            TraceSystem.traceThrowable(e);
            return def;
        }
    }

    /**
     * Get an int property value from a properties object.
     *
     * @param prop the properties object
     * @param key the key
     * @param def the default value
     * @return the value if set, or the default value if not
     */
    public static int getIntProperty(Properties prop, String key, int def) {
        String value = prop.getProperty(key, ""+def);
        try {
            return MathUtils.decodeInt(value);
        } catch (Exception e) {
            TraceSystem.traceThrowable(e);
            return def;
        }
    }

    /**
     * Load a properties object from a file.
     *
     * @param fileName the name of the properties file
     * @return the properties object
     */
    public static synchronized SortedProperties loadProperties(String fileName) throws IOException {
        SortedProperties prop = new SortedProperties();
        if (FileUtils.exists(fileName)) {
            InputStream in = null;
            try {
                in = FileUtils.openFileInputStream(fileName);
                prop.load(in);
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
        return prop;
    }

    /**
     * Store a properties file. The header and the date is not written.
     *
     * @param fileName the target file name
     */
    public synchronized void store(String fileName) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        store(out, null);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        InputStreamReader reader = new InputStreamReader(in, "ISO8859-1");
        LineNumberReader r = new LineNumberReader(reader);
        Writer w;
        try {
            w = new OutputStreamWriter(FileUtils.openFileOutputStream(fileName, false));
        } catch (SQLException e) {
            throw Message.convertToIOException(e);
        }
        PrintWriter writer = new PrintWriter(new BufferedWriter(w));
        while (true) {
            String line = r.readLine();
            if (line == null) {
                break;
            }
            if (!line.startsWith("#")) {
                writer.print(line + "\n");
            }
        }
        writer.close();
    }

}
