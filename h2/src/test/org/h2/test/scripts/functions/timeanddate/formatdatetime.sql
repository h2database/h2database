-- Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CALL FORMATDATETIME(PARSEDATETIME('2001-02-03 04:05:06 GMT', 'yyyy-MM-dd HH:mm:ss z', 'en', 'GMT'), 'EEE, d MMM yyyy HH:mm:ss z', 'en', 'GMT');
> 'Sat, 3 Feb 2001 04:05:06 GMT'
> ------------------------------
> Sat, 3 Feb 2001 04:05:06 GMT
> rows: 1

CALL FORMATDATETIME(TIMESTAMP '2001-02-03 04:05:06', 'yyyy-MM-dd HH:mm:ss');
> '2001-02-03 04:05:06'
> ---------------------
> 2001-02-03 04:05:06
> rows: 1

CALL FORMATDATETIME(TIMESTAMP '2001-02-03 04:05:06', 'MM/dd/yyyy HH:mm:ss');
> '02/03/2001 04:05:06'
> ---------------------
> 02/03/2001 04:05:06
> rows: 1

CALL FORMATDATETIME(TIMESTAMP '2001-02-03 04:05:06', 'd. MMMM yyyy', 'de');
> '3. Februar 2001'
> -----------------
> 3. Februar 2001
> rows: 1

CALL FORMATDATETIME(PARSEDATETIME('Sat, 3 Feb 2001 04:05:06 GMT', 'EEE, d MMM yyyy HH:mm:ss z', 'en', 'GMT'), 'yyyy-MM-dd HH:mm:ss', 'en', 'GMT');
> '2001-02-03 04:05:06'
> ---------------------
> 2001-02-03 04:05:06
> rows: 1
