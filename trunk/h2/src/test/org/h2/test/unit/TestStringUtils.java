/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Date;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.util.StringUtils;

/**
 * Tests string utility methods.
 */
public class TestStringUtils extends TestBase {

    public void test() throws Exception {
        testXML();
        testSplit();
        testJavaString();
        testURL();
        testPad();
    }

    private void testPad() throws Exception {
        check("large", StringUtils.pad("larger text", 5, null, true));
        check("large", StringUtils.pad("larger text", 5, null, false));
        check("short+++++", StringUtils.pad("short", 10, "+", true));
        check("+++++short", StringUtils.pad("short", 10, "+", false));
    }

    private void testXML() throws Exception {
        check("<!-- - - - - - -abc- - - - - - -->\n", StringUtils.xmlComment("------abc------"));
        check("<test/>\n", StringUtils.xmlNode("test", null, null));
        check("<test>Gr&#xfc;bel</test>\n", StringUtils.xmlNode("test", null, StringUtils.xmlText("Gr\u00fcbel")));
        check("Rand&amp;Blue", StringUtils.xmlText("Rand&Blue"));
        check("&lt;&lt;[[[]]]&gt;&gt;", StringUtils.xmlCData("<<[[[]]]>>"));
        Date dt = StringUtils.parseDateTime("2001-02-03 04:05:06 GMT", "yyyy-MM-dd HH:mm:ss z", "en", "GMT");
        String s = StringUtils.xmlStartDoc()
                + StringUtils.xmlComment("Test Comment")
                + StringUtils.xmlNode("rss", StringUtils.xmlAttr("version", "2.0"), StringUtils
                        .xmlComment("Test Comment\nZeile2")
                        + StringUtils.xmlNode("channel", null, StringUtils.xmlNode("title", null, "H2 Database Engine")
                                + StringUtils.xmlNode("link", null, "http://www.h2database.com")
                                + StringUtils.xmlNode("description", null, "H2 Database Engine")
                                + StringUtils.xmlNode("language", null, "en-us")
                                + StringUtils.xmlNode("pubDate", null, StringUtils.formatDateTime(dt,
                                        "EEE, d MMM yyyy HH:mm:ss z", "en", "GMT"))
                                + StringUtils.xmlNode("lastBuildDate", null, StringUtils.formatDateTime(dt,
                                        "EEE, d MMM yyyy HH:mm:ss z", "en", "GMT"))
                                + StringUtils.xmlNode("item", null, StringUtils.xmlNode("title", null,
                                        "New Version 0.9.9.9.9")
                                        + StringUtils.xmlNode("link", null, "http://www.h2database.com")
                                        + StringUtils.xmlNode("description", null, StringUtils
                                                .xmlCData("\nNew Features\nTest\n")))));
        check(s, "<?xml version=\"1.0\"?>\n" + "<!-- Test Comment -->\n" + "<rss version=\"2.0\">\n" + "    <!--\n"
                + "        Test Comment\n" + "        Zeile2\n" + "    -->\n" + "    <channel>\n"
                + "        <title>H2 Database Engine</title>\n" + "        <link>http://www.h2database.com</link>\n"
                + "        <description>H2 Database Engine</description>\n" + "        <language>en-us</language>\n"
                + "        <pubDate>Sat, 3 Feb 2001 04:05:06 GMT</pubDate>\n"
                + "        <lastBuildDate>Sat, 3 Feb 2001 04:05:06 GMT</lastBuildDate>\n" + "        <item>\n"
                + "            <title>New Version 0.9.9.9.9</title>\n"
                + "            <link>http://www.h2database.com</link>\n" + "            <description>\n"
                + "                <![CDATA[\n" + "                New Features\n" + "                Test\n"
                + "                ]]>\n" + "            </description>\n" + "        </item>\n" + "    </channel>\n"
                + "</rss>\n");
    }

    private void testURL() throws Exception {
        Random random = new Random(1);
        for (int i = 0; i < 100; i++) {
            int len = random.nextInt(10);
            StringBuffer buff = new StringBuffer();
            for (int j = 0; j < len; j++) {
                if (random.nextBoolean()) {
                    buff.append((char) random.nextInt(0x3000));
                } else {
                    buff.append((char) random.nextInt(255));
                }
            }
            String a = buff.toString();
            String b = URLEncoder.encode(a, "UTF-8");
            String c = URLDecoder.decode(b, "UTF-8");
            check(a, c);
            String d = StringUtils.urlDecode(b);
            check(d, c);
        }
    }

    private void testJavaString() throws Exception {
        Random random = new Random(1);
        for (int i = 0; i < 1000; i++) {
            int len = random.nextInt(10);
            StringBuffer buff = new StringBuffer();
            for (int j = 0; j < len; j++) {
                if (random.nextBoolean()) {
                    buff.append((char) random.nextInt(0x3000));
                } else {
                    buff.append((char) random.nextInt(255));
                }
            }
            String a = buff.toString();
            String b = StringUtils.javaEncode(a);
            String c = StringUtils.javaDecode(b);
            check(a, c);
        }
    }

    private void testSplit() throws Exception {
        check(3, StringUtils.arraySplit("ABC,DEF,G\\,HI", ',', false).length);
        check(StringUtils.arrayCombine(new String[] { "", " ", "," }, ','), ", ,\\,");
        Random random = new Random(1);
        for (int i = 0; i < 100; i++) {
            int len = random.nextInt(10);
            StringBuffer buff = new StringBuffer();
            String select = "abcd,";
            for (int j = 0; j < len; j++) {
                char c = select.charAt(random.nextInt(select.length()));
                if (c == 'a') {
                    buff.append("\\\\");
                } else if (c == 'b') {
                    buff.append("\\,");
                } else {
                    buff.append(c);
                }
            }
            String a = buff.toString();
            String[] b = StringUtils.arraySplit(a, ',', false);
            String c = StringUtils.arrayCombine(b, ',');
            check(a, c);
        }
    }

}
