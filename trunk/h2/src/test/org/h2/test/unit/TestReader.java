/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;

import org.h2.test.TestBase;
import org.h2.util.IOUtils;
import org.h2.util.TypeConverter;

public class TestReader extends TestBase {

    public void test() throws Exception {
        String s = "\u00ef\u00f6\u00fc";
        StringReader r = new StringReader(s);
        InputStream in = TypeConverter.getInputStream(r);
        byte[] buff = IOUtils.readBytesAndClose(in, 0);
        InputStream in2 = new ByteArrayInputStream(buff);
        Reader r2 = TypeConverter.getReader(in2);
        String s2 = IOUtils.readStringAndClose(r2, Integer.MAX_VALUE);
        check(s2, "\u00ef\u00f6\u00fc");
    }

}
