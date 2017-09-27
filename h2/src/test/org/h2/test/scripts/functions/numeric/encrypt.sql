call encrypt('AES', '00000000000000000000000000000000', stringtoutf8('Hello World Test'));
> X'dbd42d55d4b923c4b03eba0396fac98e'
> -----------------------------------
> dbd42d55d4b923c4b03eba0396fac98e
> rows: 1

CALL ENCRYPT('XTEA', '00', STRINGTOUTF8('Test'));
> X'8bc9a4601b3062692a72a5941072425f'
> -----------------------------------
> 8bc9a4601b3062692a72a5941072425f
> rows: 1

call encrypt('XTEA', '000102030405060708090a0b0c0d0e0f', '4142434445464748');
> X'dea0b0b40966b0669fbae58ab503765f'
> -----------------------------------
> dea0b0b40966b0669fbae58ab503765f
> rows: 1

