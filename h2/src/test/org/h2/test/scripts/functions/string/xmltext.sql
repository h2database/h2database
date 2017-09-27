CALL XMLTEXT('test');
> 'test'
> ------
> test
> rows: 1

CALL XMLTEXT('<test>');
> '&lt;test&gt;'
> --------------
> &lt;test&gt;
> rows: 1

SELECT XMLTEXT('hello' || chr(10) || 'world') X;
> X
> -----------
> hello world
> rows: 1

CALL XMLTEXT('hello' || chr(10) || 'world', true);
> 'hello&#xa;world'
> -----------------
> hello&#xa;world
> rows: 1

