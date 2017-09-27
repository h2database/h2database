create memory table test(id int primary key, name varchar(255));
> ok

INSERT INTO TEST VALUES(2, STRINGDECODE('abcsond\344rzeich\344 ') || char(22222) || STRINGDECODE(' \366\344\374\326\304\334\351\350\340\361!'));
> update count: 1

call STRINGENCODE(STRINGDECODE('abcsond\344rzeich\344 \u56ce \366\344\374\326\304\334\351\350\340\361!'));
> 'abcsond\u00e4rzeich\u00e4 \u56ce \u00f6\u00e4\u00fc\u00d6\u00c4\u00dc\u00e9\u00e8\u00e0\u00f1!'
> ------------------------------------------------------------------------------------------------
> abcsond\u00e4rzeich\u00e4 \u56ce \u00f6\u00e4\u00fc\u00d6\u00c4\u00dc\u00e9\u00e8\u00e0\u00f1!
> rows: 1


CALL STRINGENCODE(STRINGDECODE('Lines 1\nLine 2'));
> 'Lines 1\nLine 2'
> -----------------
> Lines 1\nLine 2
> rows: 1

