CALL PARSEDATETIME('3. Februar 2001', 'd. MMMM yyyy', 'de');
> TIMESTAMP '2001-02-03 00:00:00.0'
> ---------------------------------
> 2001-02-03 00:00:00.0
> rows: 1

CALL PARSEDATETIME('02/03/2001 04:05:06', 'MM/dd/yyyy HH:mm:ss');
> TIMESTAMP '2001-02-03 04:05:06.0'
> ---------------------------------
> 2001-02-03 04:05:06.0
> rows: 1

