/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: Robert Rathsack (firstName dot lastName at gmx dot de)
 */
package org.h2.test.unit;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.h2.test.TestBase;
import org.h2.util.DateTimeUtils;

/**
 * Test cases for DateTimeIso8601Utils.
 */
public class TestDateIso8601 extends TestBase {

    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testIsoDayOfWeek();
        testIsoWeekJanuary1thMonday();
        testIsoWeekJanuary1thTuesday();
        testIsoWeekJanuary1thWednesday();
        testIsoWeekJanuary1thThursday();
        testIsoWeekJanuary1thFriday();
        testIsoWeekJanuary1thSaturday();
        testIsoWeekJanuary1thSunday();
        testIsoYearJanuary1thMonday();
        testIsoYearJanuary1thTuesday();
        testIsoYearJanuary1thWednesday();
        testIsoYearJanuary1thThursday();
        testIsoYearJanuary1thFriday();
        testIsoYearJanuary1thSaturday();
        testIsoYearJanuary1thSunday();
    }

    private Date parse(String s) throws ParseException {
        return dateFormatter.parse(s);
    }

    private static int getIsoDayOfWeek(Date date) {
        return DateTimeUtils.getIsoDayOfWeek(date);
    }

    /**
     * Test if day of week is returned as Monday = 1 to Sunday = 7.
     */
    private void testIsoDayOfWeek() throws Exception {
        assertEquals(1, getIsoDayOfWeek(parse("2008-09-29")));
        assertEquals(2, getIsoDayOfWeek(parse("2008-09-30")));
        assertEquals(3, getIsoDayOfWeek(parse("2008-10-01")));
        assertEquals(4, getIsoDayOfWeek(parse("2008-10-02")));
        assertEquals(5, getIsoDayOfWeek(parse("2008-10-03")));
        assertEquals(6, getIsoDayOfWeek(parse("2008-10-04")));
        assertEquals(7, getIsoDayOfWeek(parse("2008-10-05")));
    }

    private static int getIsoWeek(Date date) {
        Timestamp ts = new Timestamp(date.getTime());
        return DateTimeUtils.getIsoWeek(ts);
    }

    /**
     * January 1st is a Monday therefore the week belongs to the next year.
     */
    private void testIsoWeekJanuary1thMonday() throws Exception {
        assertEquals(52, getIsoWeek(parse("2006-12-31")));
        assertEquals(1, getIsoWeek(parse("2007-01-01")));
        assertEquals(1, getIsoWeek(parse("2007-01-07")));
        assertEquals(2, getIsoWeek(parse("2007-01-08")));
    }

    /**
     * January 1st is a Tuesday therefore the week belongs to the next year.
     */
    private void testIsoWeekJanuary1thTuesday() throws Exception {
        assertEquals(52, getIsoWeek(parse("2007-12-30")));
        assertEquals(1, getIsoWeek(parse("2007-12-31")));
        assertEquals(1, getIsoWeek(parse("2008-01-01")));
        assertEquals(1, getIsoWeek(parse("2008-01-06")));
        assertEquals(2, getIsoWeek(parse("2008-01-07")));
    }

    /**
     * January1th is a Wednesday therefore the week belongs to the next year.
     */
    private void testIsoWeekJanuary1thWednesday() throws Exception {
        assertEquals(52, getIsoWeek(parse("2002-12-28")));
        assertEquals(52, getIsoWeek(parse("2002-12-29")));
        assertEquals(1, getIsoWeek(parse("2002-12-30")));
        assertEquals(1, getIsoWeek(parse("2002-12-31")));
        assertEquals(1, getIsoWeek(parse("2003-01-01")));
        assertEquals(1, getIsoWeek(parse("2003-01-05")));
        assertEquals(2, getIsoWeek(parse("2003-01-06")));
    }

    /**
     * January 1st is a Thursday therefore the week belongs to the next year.
     */
    private void testIsoWeekJanuary1thThursday() throws Exception {
        assertEquals(52, getIsoWeek(parse("2008-12-28")));
        assertEquals(1, getIsoWeek(parse("2008-12-29")));
        assertEquals(1, getIsoWeek(parse("2008-12-30")));
        assertEquals(1, getIsoWeek(parse("2008-12-31")));
        assertEquals(1, getIsoWeek(parse("2009-01-01")));
        assertEquals(1, getIsoWeek(parse("2009-01-04")));
        assertEquals(2, getIsoWeek(parse("2009-01-09")));
    }

    /**
     * January 1st is a Friday therefore the week belongs to the previous year.
     */
    private void testIsoWeekJanuary1thFriday() throws Exception {
        assertEquals(53, getIsoWeek(parse("2009-12-31")));
        assertEquals(53, getIsoWeek(parse("2010-01-01")));
        assertEquals(53, getIsoWeek(parse("2010-01-03")));
        assertEquals(1, getIsoWeek(parse("2010-01-04")));
    }

    /**
     * January 1st is a Saturday therefore the week belongs to the previous
     * year.
     */
    private void testIsoWeekJanuary1thSaturday() throws Exception {
        assertEquals(52, getIsoWeek(parse("2010-12-31")));
        assertEquals(52, getIsoWeek(parse("2011-01-01")));
        assertEquals(52, getIsoWeek(parse("2011-01-02")));
        assertEquals(1, getIsoWeek(parse("2011-01-03")));
    }

    /**
     * January 1st is a Sunday therefore the week belongs to the previous year.
     */
    private void testIsoWeekJanuary1thSunday() throws Exception {
        assertEquals(52, getIsoWeek(parse("2011-12-31")));
        assertEquals(52, getIsoWeek(parse("2012-01-01")));
        assertEquals(1, getIsoWeek(parse("2012-01-02")));
        assertEquals(1, getIsoWeek(parse("2012-01-08")));
        assertEquals(2, getIsoWeek(parse("2012-01-09")));
    }

    private static int getIsoYear(Date date) {
        Timestamp ts = new Timestamp(date.getTime());
        return DateTimeUtils.getIsoYear(ts);
    }

    /**
     * January 1st is a Monday therefore year is equal to isoYear.
     */
    private void testIsoYearJanuary1thMonday() throws Exception {
        assertEquals(2006, getIsoYear(parse("2006-12-28")));
        assertEquals(2006, getIsoYear(parse("2006-12-29")));
        assertEquals(2006, getIsoYear(parse("2006-12-30")));
        assertEquals(2006, getIsoYear(parse("2006-12-31")));
        assertEquals(2007, getIsoYear(parse("2007-01-01")));
        assertEquals(2007, getIsoYear(parse("2007-01-02")));
        assertEquals(2007, getIsoYear(parse("2007-01-03")));
    }

    /**
     * January 1st is a Tuesday therefore 31th of December belong to the next
     * year.
     */
    private void testIsoYearJanuary1thTuesday() throws Exception {
        assertEquals(2007, getIsoYear(parse("2007-12-28")));
        assertEquals(2007, getIsoYear(parse("2007-12-29")));
        assertEquals(2007, getIsoYear(parse("2007-12-30")));
        assertEquals(2008, getIsoYear(parse("2007-12-31")));
        assertEquals(2008, getIsoYear(parse("2008-01-01")));
        assertEquals(2008, getIsoYear(parse("2008-01-02")));
        assertEquals(2008, getIsoYear(parse("2008-01-03")));
        assertEquals(2008, getIsoYear(parse("2008-01-04")));
    }

    /**
     * January 1st is a Wednesday therefore 30th and 31th of December belong to
     * the next year.
     */
    private void testIsoYearJanuary1thWednesday() throws Exception {
        assertEquals(2002, getIsoYear(parse("2002-12-28")));
        assertEquals(2002, getIsoYear(parse("2002-12-29")));
        assertEquals(2003, getIsoYear(parse("2002-12-30")));
        assertEquals(2003, getIsoYear(parse("2002-12-31")));
        assertEquals(2003, getIsoYear(parse("2003-01-01")));
        assertEquals(2003, getIsoYear(parse("2003-01-02")));
        assertEquals(2003, getIsoYear(parse("2003-12-02")));
    }

    /**
     * January 1st is a Thursday therefore 29th - 31th of December belong to the
     * next year.
     */
    private void testIsoYearJanuary1thThursday() throws Exception {
        assertEquals(2008, getIsoYear(parse("2008-12-28")));
        assertEquals(2009, getIsoYear(parse("2008-12-29")));
        assertEquals(2009, getIsoYear(parse("2008-12-30")));
        assertEquals(2009, getIsoYear(parse("2008-12-31")));
        assertEquals(2009, getIsoYear(parse("2009-01-01")));
        assertEquals(2009, getIsoYear(parse("2009-01-02")));
        assertEquals(2009, getIsoYear(parse("2009-01-03")));
        assertEquals(2009, getIsoYear(parse("2009-01-04")));
    }

    /**
     * January 1st is a Friday therefore 1st - 3rd of January belong to the
     * previous year.
     */
    private void testIsoYearJanuary1thFriday() throws Exception {
        assertEquals(2009, getIsoYear(parse("2009-12-28")));
        assertEquals(2009, getIsoYear(parse("2009-12-29")));
        assertEquals(2009, getIsoYear(parse("2009-12-30")));
        assertEquals(2009, getIsoYear(parse("2009-12-31")));
        assertEquals(2009, getIsoYear(parse("2010-01-01")));
        assertEquals(2009, getIsoYear(parse("2010-01-02")));
        assertEquals(2009, getIsoYear(parse("2010-01-03")));
        assertEquals(2010, getIsoYear(parse("2010-01-04")));
    }

    /**
     * January 1st is a Saturday therefore 1st and 2nd of January belong to the
     * previous year.
     */
    private void testIsoYearJanuary1thSaturday() throws Exception {
        assertEquals(2010, getIsoYear(parse("2010-12-28")));
        assertEquals(2010, getIsoYear(parse("2010-12-29")));
        assertEquals(2010, getIsoYear(parse("2010-12-30")));
        assertEquals(2010, getIsoYear(parse("2010-12-31")));
        assertEquals(2010, getIsoYear(parse("2011-01-01")));
        assertEquals(2010, getIsoYear(parse("2011-01-02")));
        assertEquals(2011, getIsoYear(parse("2011-01-03")));
        assertEquals(2011, getIsoYear(parse("2011-01-04")));
    }

    /**
     * January 1st is a Sunday therefore this day belong to the previous year.
     */
    private void testIsoYearJanuary1thSunday() throws Exception {
        assertEquals(2011, getIsoYear(parse("2011-12-28")));
        assertEquals(2011, getIsoYear(parse("2011-12-29")));
        assertEquals(2011, getIsoYear(parse("2011-12-30")));
        assertEquals(2011, getIsoYear(parse("2011-12-31")));
        assertEquals(2011, getIsoYear(parse("2012-01-01")));
        assertEquals(2012, getIsoYear(parse("2012-01-02")));
        assertEquals(2012, getIsoYear(parse("2012-01-03")));
        assertEquals(2012, getIsoYear(parse("2012-01-04")));
    }

}
