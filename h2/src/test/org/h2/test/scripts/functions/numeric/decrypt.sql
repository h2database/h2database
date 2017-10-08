call utf8tostring(decrypt('AES', '00000000000000000000000000000000', 'dbd42d55d4b923c4b03eba0396fac98e'));
> 'Hello World Test'
> ------------------
> Hello World Test
> rows: 1

call utf8tostring(decrypt('AES', hash('sha256', stringtoutf8('Hello'), 1000), encrypt('AES', hash('sha256', stringtoutf8('Hello'), 1000), stringtoutf8('Hello World Test'))));
> 'Hello World Test'
> ------------------
> Hello World Test
> rows: 1

